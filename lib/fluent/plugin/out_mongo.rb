module Fluent


class MongoOutput < BufferedOutput
  Fluent::Plugin.register_output('mongo', self)

  include SetTagKeyMixin
  config_set_default :include_tag_key, false

  include SetTimeKeyMixin
  config_set_default :include_time_key, true

  config_param :database, :string
  config_param :collection, :string, :default => nil
  config_param :tag_collection, :string, :default => nil
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 27017

  attr_reader :argument

  def initialize
    super
    require 'mongo'
    require 'msgpack'

    @argument = {:capped => false}
    @collections = {}  # collection_name => Mongo::Collection
  end

  def configure(conf)
    super

    if col = @collection
      @collection_proc = Proc.new {|tag| col }
    elsif remove_prefix = @tag_collection
      if remove_prefix.empty?
        @collection_proc = Proc.new {|tag| tag }
      else
        regexp = /^#{Regexp.escape(remove_prefix)}\.?(.*)/
        @collection_proc = Proc.new {|tag| m = regepx.match(tag) and m[1] }
      end
    else
      raise ConfigError, "'collection' or 'tag_collection' parameter is required on mongo output"
    end

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
    @client = Mongo::Connection.new(@host, @port).db(@database)
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @client.connection.close
    super
  end

  def emit(tag, es, chain)
    if collection_name = @collection_proc.call(tag)
      super(tag, es, chain, collection_name)
    end
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
    operate(chunk.key, records)
  end

  private

  def get_or_create_collection(collection_name)
    unless collection = @collections[collection_name]
      if @client.collection_names.include?(collection_name)
        collection = @client.collection(collection_name)
      else
        collection = @client.create_collection(collection_name, @argument)
      end
      @collections[collection_name] = collection
    end

    return collection if @argument[:capped] == collection.capped? # TODO: Verify capped configuration

    # raise Exception if old collection does not match lastest configuration
    raise ConfigError, "New configuration is different from existing collection"
  end

  def operate(collection_name, records)
    get_or_create_collection(collection_name).insert(records)
  end
end


end
