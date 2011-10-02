module Fluent


class MongoOutput < BufferedOutput
  Fluent::Plugin.register_output('mongo', self)

  def initialize
    super
    require 'mongo'
    require 'msgpack'

    # Sub-class can overwrite following parameters
    @database_name = nil
    @collection_name = nil
  end

  def configure(conf)
    super

    @database_name = conf['database'] if conf.has_key?('database')
    @collection_name = conf['collection'] if conf.has_key?('collection')
    raise ConfigError, "'database' and 'collection' parameter is required on mongo output" if @database_name.nil? || @collection_name.nil?
    @host, @port = host_and_port(conf)

    # capped configuration
    @argument = {:capped => false}
    if conf['capped']
      raise ConfigError, "'capped_size' parameter is required on <store> of Mongo output" unless conf.has_key?('capped_size')
      @argument[:capped] = true
      @argument[:size] = Config.size_value(conf['capped_size'])
      @argument[:max] = Config.size_value(conf['capped_max']) if conf.has_key?('capped_max')
    end
  end

  def start
    super
    @collection = get_or_create_collection #Mongo::Connection.new(@host, @port).db(@database_name).collection(@collection_name)
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @collection.db.connection.close
    super
  end

  def format(tag, event)
    event.record.to_msgpack
  end

  def write(chunk)
    records = []
    chunk.open { |io|
      begin
        MessagePack::Unpacker.new(io).each { |record| records << record }
      rescue EOFError
        # EOFError always occured when reached end of chunk.
      end
    }

    @collection.insert(records)
  end

  private

  def host_and_port(conf)
    host = conf['host'] || 'localhost'
    port = conf['port'] || 27017
    [host, Integer(port)]
  end

  def get_or_create_collection
    db = Mongo::Connection.new(@host, @port).db(@database_name)
    if db.collection_names.include?(@collection_name)
      collection = db.collection(@collection_name)
      return collection if @argument[:capped] == collection.capped? # TODO: Verify capped configuration

      # Drop if old collection does not match lastest configuration
      collection.drop
    end

    db.create_collection(@collection_name, @argument)
  end
end


end
