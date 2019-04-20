# coding: utf-8
require 'mongo'
require 'bson'
require 'fluent/plugin/input'
require 'fluent/plugin/mongo_auth'
require 'fluent/plugin/logger_support'

module Fluent::Plugin
  class MongoTailInput < Input
    Fluent::Plugin.register_input('mongo_tail', self)

    helpers :timer

    include Fluent::MongoAuthParams
    include Fluent::MongoAuth
    include Fluent::LoggerSupport

    desc "MongoDB database"
    config_param :database, :string, default: nil
    desc "MongoDB collection"
    config_param :collection, :string
    desc "MongoDB host"
    config_param :host, :string, default: 'localhost'
    desc "MongoDB port"
    config_param :port, :integer, default: 27017
    desc "Tailing interval"
    config_param :wait_time, :integer, default: 1
    desc "MongoDB node URL"
    config_param :url, :string, default: nil

    desc "Input tag"
    config_param :tag, :string, default: nil
    desc "Treat key as tag"
    config_param :tag_key, :string, default: nil
    desc "Treat key as time"
    config_param :time_key, :string, default: nil
    desc "Time format"
    config_param :time_format, :string, default: nil
    config_param :object_id_keys, :array, default: nil

    desc "To store last ObjectID"
    config_param :id_store_file, :string, default: nil

    desc "SSL connection"
    config_param :ssl, :bool, default: false

    desc "Batch size for each find"
    config_param :batch_size, :integer, default: nil

    def initialize
      super

      @client_options = {}
      @connection_options = {}
    end

    def configure(conf)
      super

      if !@tag and !@tag_key
        raise Fluent::ConfigError, "'tag' or 'tag_key' option is required on mongo_tail input"
      end

      if @database && @url
        raise Fluent::ConfigError, "Both 'database' and 'url' can not be set"
      end

      if !@database && !@url
        raise Fluent::ConfigError, "One of 'database' or 'url' must be specified"
      end

      @last_id = @id_store_file ? get_last_id : nil
      @connection_options[:ssl] = @ssl

      if @batch_size && @batch_size <= 0
        raise Fluent::ConfigError, "Batch size must be positive."
      end

      configure_logger(@mongo_log_level)
    end

    def start
      super

      @file = get_id_store_file if @id_store_file
      @collection = get_collection
      # Resume tailing from last inserted id.
      # Because tailable option is obsoleted since mongo driver 2.0.
      @last_id = get_last_inserted_id if !@id_store_file and get_last_inserted_id
      timer_execute(:in_mongo_tail_watcher, @wait_time, &method(:run))
    end

    def shutdown
      if @id_store_file
        save_last_id
        @file.close
      end

      @client.close

      super
    end

    def run
      option = {}
      begin
        option['_id'] = {'$gt' => BSON::ObjectId(@last_id)} if @last_id
        documents = @collection.find(option)
        documents = documents.limit(@batch_size) if @batch_size
        if documents.count >= 1
          process_documents(documents)
        end
      rescue
        # ignore Exceptions
      end
    end

    private

    def client
      @client_options[:database] = @database
      @client_options[:user] = @user if @user
      @client_options[:password] = @password if @password
      
      if @database
       Mongo::Client.new(["#{node_string}"], @client_options)
      end

      if @url
       Mongo::Client.new("#{@url}")
      end
    end

    def get_collection
      @client = client
      @client = authenticate(@client)
      @client["#{@collection}"]
    end

    def node_string
      case
      when @database
        "#{@host}:#{@port}"
      when @url
        @url
      end
    end

    def process_documents(documents)
      es = Fluent::MultiEventStream.new
      documents.each {|doc|
        time = if @time_key
                 t = doc.delete(@time_key)
                 t.nil? ? Fluent::Engine.now : t.to_i
               else
                 Fluent::Engine.now
               end
        @tag = if @tag_key
                t = doc.delete(@tag_key)
                t.nil? ? 'mongo.missing_tag' : t
              else
                @tag
              end
        if @object_id_keys
          @object_id_keys.each {|id_key|
            doc[id_key] = doc[id_key].to_s
          }
        end

        if id = doc.delete('_id')
          @last_id = id.to_s
          doc['_id_str'] = @last_id
          save_last_id if @id_store_file
        end
        es.add(time, doc)
      }
      router.emit_stream(@tag, es)
    end

    def get_last_inserted_id
      last_inserted_id = nil
      documents = @collection.find()
      if documents.count >= 1
        documents.each {|doc|
          if id = doc.delete('_id')
            last_inserted_id = id
          end
        }
      end
      last_inserted_id
    end

    def get_id_store_file
      file = File.open(@id_store_file, 'w')
      file.sync
      file
    end

    def get_last_id
      if File.exist?(@id_store_file)
        BSON::ObjectId(File.read(@id_store_file)).to_s rescue nil
      else
        nil
      end
    end

    def save_last_id
      @file.pos = 0
      @file.write(@last_id)
    end
  end
end
