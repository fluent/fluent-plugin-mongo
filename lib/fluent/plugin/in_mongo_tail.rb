require 'fluent/input'

module Fluent
  class MongoTailInput < Input
    Plugin.register_input('mongo_tail', self)

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    # Define `router` method of v0.12 to support v0.10 or earlier
    unless method_defined?(:router)
      define_method("router") { ::Fluent::Engine }
    end

    require 'fluent/plugin/mongo_auth'
    include MongoAuthParams
    include MongoAuth
    require 'fluent/plugin/logger_support'
    include LoggerSupport

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

    desc "To store last ObjectID"
    config_param :id_store_file, :string, default: nil

    desc "SSL connection"
    config_param :ssl, :bool, default: false

    def initialize
      super
      require 'mongo'
      require 'bson'

      @client_options = {}
      @connection_options = {}
    end

    def configure(conf)
      super

      if !@tag and !@tag_key
        raise ConfigError, "'tag' or 'tag_key' option is required on mongo_tail input"
      end

      if @database && @url
        raise ConfigError, "Both 'database' and 'url' can not be set"
      end

      if !@database && !@url
        raise ConfigError, "One of 'database' or 'url' must be specified"
      end

      @last_id = @id_store_file ? get_last_id : nil
      @connection_options[:ssl] = @ssl

      configure_logger(@mongo_log_level)
    end

    def start
      super

      @file = get_id_store_file if @id_store_file
      @collection = get_collection
      # Resume tailing from last inserted id.
      # Because tailable option is obsoleted since mongo driver 2.0.
      @last_id = get_last_inserted_id if !@id_store_file and get_last_inserted_id
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      if @id_store_file
        save_last_id
        @file.close
      end

      @stop = true
      @thread.join
      @client.close

      super
    end

    def run
      loop {
        option = {}
        begin
          loop {
            return if @stop

            option['_id'] = {'$gt' => BSON::ObjectId(@last_id)} if @last_id
            documents = @collection.find(option)
            if documents.count >= 1
              process_documents(documents)
            else
              sleep @wait_time
            end
          }
        rescue
          # ignore Exceptions
        end
      }
    end

    private

    def client
      @client_options[:database] = @database
      @client_options[:user] = @user if @user
      @client_options[:password] = @password if @password
      Mongo::Client.new(["#{node_string}"], @client_options)
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
      es = MultiEventStream.new
      documents.each {|doc|
        time = if @time_key
                 t = doc.delete(@time_key)
                 t.nil? ? Engine.now : t.to_i
               else
                 Engine.now
               end
        tag = if @tag_key
                t = doc.delete(@tag_key)
                t.nil? ? 'mongo.missing_tag' : t
              else
                @tag
              end
        if id = doc.delete('_id')
          @last_id = id.to_s
          doc['_id_str'] = @last_id
          save_last_id if @id_store_file
        end
        es.add(time, doc)
      }
      router.emit_stream(tag, es)
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
