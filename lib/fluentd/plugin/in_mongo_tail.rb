require 'fluentd/plugin/input'

module Fluentd
  module Plugin
  class MongoTailInput < Input
    Plugin.register_input('mongo_tail', self)

    require_relative 'mongo_util'
    include MongoUtil

    config_param :database, :string
    config_param :collection, :string
    config_param :host, :string, :default => 'localhost'
    config_param :port, :integer, :default => 27017
    config_param :wait_time, :integer, :default => 1

    config_param :tag, :string, :default => nil
    config_param :tag_key, :string, :default => nil
    config_param :time_key, :string, :default => nil
    config_param :time_format, :string, :default => nil

    # To store last ObjectID
    config_param :id_store_file, :string, :default => nil

    def initialize
      super
      require 'mongo'
      require 'bson'
    end

    def configure(conf)
      super

      if !@tag and !@tag_key
        raise ConfigError, "'tag' or 'tag_key' option is required on mongo_tail input"
      end

      @last_id = @id_store_file ? get_last_id : nil

      Fluentd.log.debug "Setup mongo_tail configuration: mode = #{@id_store_file ? 'persistent' : 'non-persistent'}"
    end

    def start
      super
      @file = get_id_store_file if @id_store_file
      @client = get_capped_collection
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      if @id_store_file
        save_last_id
        @file.close
      end

      @thread.join
      @client.db.connection.close
      super
    end

    def run
      loop {
        tailoop(Mongo::Cursor.new(@client, cursor_conf))
      }
    end

    private

    def get_capped_collection
      begin
        db = authenticate(Mongo::Connection.new(@host, @port).db(@database))
        raise ConfigError, "'#{@database}.#{@collection}' not found: node = #{@host}:#{@port}" unless db.collection_names.include?(@collection)
        collection = db.collection(@collection)
        raise ConfigError, "'#{@database}.#{@collection}' is not capped: node = #{@host}:#{@port}" unless collection.capped?
        collection
      rescue Mongo::ConnectionFailure => e
        Fluentd.log.fatal "Failed to connect to 'mongod'. Please restart 'fluentd' after 'mongod' started: #{e}"
        exit!
      rescue Mongo::OperationFailure => e
        Fluentd.log.fatal "Operation failed. Probably, 'mongod' needs an authentication: #{e}"
        exit!
      end
    end

    def tailoop(cursor)
      loop {
        cursor = Mongo::Cursor.new(@client, cursor_conf) unless cursor.alive?
        if doc = cursor.next_document
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

          # Should use MultiEventStream?
          collector.emit(tag, time, doc)
        else
          sleep @wait_time
        end
      }
    rescue
      # ignore Mongo::OperationFailuer at CURSOR_NOT_FOUND
    end

    def cursor_conf
      conf = {}
      conf[:tailable] = true
      conf[:selector] = {'_id' => {'$gt' => BSON::ObjectId(@last_id)}} if @last_id
      conf
    end

    # following methods are used when id_store_file is true

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
end
