module Fluent


class MongoOutput < BufferedOutput
  Fluent::Plugin.register_output('mongo', self)

  def initialize
    super
    require 'mongo'
    require 'msgpack'
  end

  def configure(conf)
    super

    raise ConfigError, "'database' parameter is required on Mongo output"   unless @database_name = conf['database']
    raise ConfigError, "'collection' parameter is required on Mongo output" unless @collection_name = conf['collection']

    @host = conf['host'] || 'localhost'
    @port = conf['port'] || 27017
    @port = Integer(@port)
  end

  def start
    super
    @collection = Mongo::Connection.new(@host, @port).db(@database_name).collection(@collection_name)
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
end


end
