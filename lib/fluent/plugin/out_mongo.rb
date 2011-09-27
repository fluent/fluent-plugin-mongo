module Fluent


class MongoOutput < Output
  Fluent::Plugin.register_output('mongo', self)

  def initialize
    super
    require 'mongo'
  end

  def configure(conf)
    super

    raise ConfigError, "'database' parameter is required on file output"   unless @database = conf['database']
    raise ConfigError, "'collection' parameter is required on file output" unless @collection = conf['collection']

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

  def emit(tag, event_stream, chain)
    event_stream.each { |event|
      @collection.insert(event.record)
    }

    chain.next
  end
end


end
