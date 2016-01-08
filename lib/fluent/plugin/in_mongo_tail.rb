module Fluent
  class MongoTailInput < Input
    Plugin.register_input('mongo_tail', self)

    require 'fluent/plugin/mongo_util'
    include MongoUtil

    config_param :database, :string, :default => nil
    config_param :collection, :string
    config_param :host, :string, :default => 'localhost'
    config_param :port, :integer, :default => 27017
    config_param :wait_time, :integer, :default => 1
    config_param :url, :string, :default => nil

    config_param :tag, :string, :default => nil
    config_param :tag_key, :string, :default => nil
    config_param :time_key, :string, :default => nil
    config_param :time_format, :string, :default => nil

    # To store last ObjectID
    config_param :id_store_file, :string, :default => nil
    config_param :id_store_collection, :string, :default => nil

    # SSL connection
    config_param :ssl, :bool, :default => false

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    def initialize
      super
      require 'mongo'
      require 'bson'

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

      @last_id = get_last_id
      @connection_options[:ssl] = @ssl

      $log.debug "Setup mongo_tail configuration: mode = #{@id_store_file || @id_store_collection ? 'persistent' : 'non-persistent'}, last_id = #{@last_id}"
    end

    def start
      super
      open_id_storage
      @client = get_capped_collection
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      save_last_id(@last_id) unless @last_id
      close_id_storage

      @stop = true
      @thread.join
      @client.db.connection.close
      super
    end

    def run
      loop {
        cursor = Mongo::Cursor.new(@client, cursor_conf)
        begin
          loop {
            return if @stop
            
            cursor = Mongo::Cursor.new(@client, cursor_conf) unless cursor.alive?
            if doc = cursor.next_document
              process_document(doc)
            else
              sleep @wait_time
            end
          }
        rescue
          # ignore Mongo::OperationFailuer at CURSOR_NOT_FOUND
        end
      }
    end

    private

    def get_capped_collection
      begin
        db = get_database
        raise ConfigError, "'#{database_name}.#{@collection}' not found: node = #{node_string}" unless db.collection_names.include?(@collection)
        collection = db.collection(@collection)
        raise ConfigError, "'#{database_name}.#{@collection}' is not capped: node = #{node_string}" unless collection.capped?
        collection
      rescue Mongo::ConnectionFailure => e
        log.fatal "Failed to connect to 'mongod'. Please restart 'fluentd' after 'mongod' started: #{e}"
        exit!
      rescue Mongo::OperationFailure => e
        log.fatal "Operation failed. Probably, 'mongod' needs an authentication: #{e}"
        exit!
      end
    end
    
    def get_database
      case
      when @database
        authenticate(Mongo::Connection.new(@host, @port, @connection_options).db(@database))
      when @url
        parser = Mongo::URIParser.new(@url)
        parser.connection.db(parser.db_name)
      end
    end
    
    def database_name
      case
      when @database
        @database
      when @url
        Mongo::URIParser.new(@url).db_name
      end
    end
    
    def node_string
      case
      when @database
        "#{@host}:#{@port}"
      when @url
        @url
      end
    end

    def process_document(doc)
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
        save_last_id(@last_id)
      end

      # Should use MultiEventStream?
      router.emit(tag, time, doc)
    end

    def cursor_conf
      conf = {}
      conf[:tailable] = true
      conf[:selector] = {'_id' => {'$gt' => BSON::ObjectId(@last_id)}} if @last_id
      conf
    end

    # following methods are used to read/write last_id
    
    def open_id_storage
      if @id_store_file
        @id_storage = File.open(@id_store_file, 'w')
        @id_storege.sync
      end
      
      if @id_store_collection
        @id_storage = get_database.collection(@id_store_collection)
      end
    end
    
    def close_id_storage
      if @id_storage.is_a?(File)
        @id_storage.close
      end
    end

    def get_last_id
      begin
        if @id_store_file && File.exist?(@id_store_file)
          return BSON::ObjectId(File.read(@id_store_file)).to_s
        end
      
        if @id_store_collection
          collection = get_database.collection(@id_store_collection)
          count = collection.find.count
          doc = collection.find.skip(count - 1).limit(1).first
          return doc && doc["last_id"]
        end
      rescue
        nil
      end
    end

    def save_last_id(last_id)
      if @id_storage.is_a?(File)
        @id_storage.pos = 0
        @id_storage.write(last_id)
      end
      
      if @id_storage.is_a?(Mongo::Collection)
        @id_storage.insert("last_id" => last_id)
      end
    end
  end
end
