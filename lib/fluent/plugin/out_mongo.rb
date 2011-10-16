module Fluent


class MongoOutput < BufferedOutput
  Fluent::Plugin.register_output('mongo', self)

  include SetTagKeyMixin
  config_set_default :include_tag_key, false

  include SetTimeKeyMixin
  config_set_default :include_time_key, true

  config_param :database, :string
  config_param :collection, :string
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 27017

  attr_reader :argument

  def initialize
    super
    require 'mongo'
    require 'msgpack'

    @argument = {:capped => false}
  end

  def configure(conf)
    super

    # capped configuration
    if conf.has_key?('capped')
      raise ConfigError, "'capped_size' parameter is required on <store> of Mongo output" unless conf.has_key?('capped_size')
      @argument[:capped] = true
      @argument[:size] = Config.size_value(conf['capped_size'])
      @argument[:max] = Config.size_value(conf['capped_max']) if conf.has_key?('capped_max')
    end

    # MongoDB uses BSON's Date for time.
    def @timef.format_nocache(time)
      time
    end
  end

  def start
    super
    @client = get_or_create_collection
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @client.db.connection.close
    super
  end

  def format(tag, time, record)
    record.to_msgpack
  end

  def write(chunk)
    records = []
    chunk.msgpack_each { |record|
      record[@time_key] = Time.at(record[@time_key]) if @include_time_key
      records << record
    }
    operate(records)
  end

  private

  def get_or_create_collection
    db = Mongo::Connection.new(@host, @port).db(@database)
    if db.collection_names.include?(@collection)
      collection = db.collection(@collection)
      return collection if @argument[:capped] == collection.capped? # TODO: Verify capped configuration

      # raise Exception if old collection does not match lastest configuration
      raise ConfigError, "New configuration is different from existing collection"
    end

    db.create_collection(@collection, @argument)
  end

  def operate(records)
    @client.insert(records)
  end
end


end
