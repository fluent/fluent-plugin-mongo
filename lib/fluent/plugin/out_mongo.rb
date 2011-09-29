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

    raise ConfigError, "'database' parameter is required on Mongo output"   unless @database = conf['database']
    raise ConfigError, "'collection' parameter is required on Mongo output" unless @collection = conf['collection']

    @host = conf.has_key?('host') ? conf['host'] : 'localhost'
    @port = conf.has_key?('port') ? conf['port'] : 27017
  end

  def start
    super
    @collection = Mongo::Connection.new(@host, @port).db(@database).collection(@collection)
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @collection.db.connection.close
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
