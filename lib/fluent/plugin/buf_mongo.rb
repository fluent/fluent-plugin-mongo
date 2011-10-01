require 'fluent/plugin/buf_memory'

module Fluent


class MongoBuffer < MemoryBuffer
  Plugin.register_buffer('mongo', self)

  def initialize
    super
    require 'mongo'
    require 'msgpack'
  end

  def configure(conf)
    super

    raise ConfigError, "'mongo_buffer_size' parameter is required on Mongo buffer" unless conf.has_key?('mongo_buffer_size')
    @capped_conf = {:capped => true}
    @capped_conf[:size] = Integer(conf['mongo_buffer_size'])
    @capped_conf[:max]  = Integer(conf['mongo_buffer_max']) if conf.has_key?('mongo_buffer_max')

    @database_name = conf['mongo_buffer_database'] || 'fluent'
    @collection_name = conf['mongo_buffer_collection'] || '__buffer'
    @host = conf['mongo_buffer_host'] || 'localhost'
    @port = conf['mongo_buffer_port'] || 27017
    @port = Integer(@port)
  end

  def start
    super
    @database = Mongo::Connection.new(@host, @port).db(@database_name)
    @collection = capped_collection
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @collection.db.connection.close
    super
  end

  def emit(key, data, chain)
    @collection.insert(MessagePack.unpack(data))
    super(key, data, chain)
  end

  private

  def capped_collection
    if @database.collection_names.include?(@collection_name)
      @database.collection(@collection_name)
    else
      @database.create_collection(@collection_name, @capped_conf)
    end
  end
end


end
