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
    @host, @port = host_and_port(conf)

    # capped configuration
    if capped_conf = conf.elements.first
      raise ConfigError, "'size' parameter is required on <store> of Mongo output" unless capped_conf.has_key?('size')
      @capped_argument = {:capped => true}
      @capped_argument[:size] = Integer(capped_conf['size'])
      @capped_argument[:max]  = Integer(capped_conf['max']) if capped_conf.has_key?('max')

      @capped_database_name = capped_conf['database'] || 'fluent'
      @capped_collection_name = capped_conf['collection'] || '__backup'
      @capped_host, @capped_port = host_and_port(capped_conf)
    end

    @backuped = false
  end

  def start
    super
    @collection = Mongo::Connection.new(@host, @port).db(@database_name).collection(@collection_name)
    @capped = capped_collection unless @capped_argument.nil?
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @collection.db.connection.close
    @capped.db.connection.close unless @capped.nil?
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

    unless @backuped or @capped.nil?
      @capped.insert(records)
      @backuped = true
    end

    @collection.insert(records)
    @backuped = false
  end

  private

  def host_and_port(conf)
    host = conf['host'] || 'localhost'
    port = conf['port'] || 27017
    [host, Integer(port)]
  end

  def capped_collection
    db = Mongo::Connection.new(@capped_host, @capped_port).db(@capped_database_name)
    if db.collection_names.include?(@capped_collection_name)
      # TODO: Verify capped configuraton
      db.collection(@capped_collection_name)
    else
      db.create_collection(@capped_collection_name, @capped_argument)
    end
  end
end


end
