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
    @clients = {}
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

    if remove_prefix_collection = conf['remove_prefix_collection']
      @remove_prefix_collection = Regexp.new('^' + Regexp.escape(remove_prefix_collection))
    end

    @tag_collection_mapping = Config.bool_value(conf['tag_collection_mapping']) if conf.has_key?('tag_collection_mapping')

    # MongoDB uses BSON's Date for time.
    def @timef.format_nocache(time)
      time
    end
  end

  def start
    super
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @clients.values.each {|client| client.db.connection.close }
    super
  end

  def format(tag, time, record)
    if @tag_collection_mapping
      [tag, record].to_msgpack
    else
      record.to_msgpack
    end
  end

  def write(chunk)
    result = if @tag_collection_mapping
      write_with_tags(chunk)
    else
      write_without_tags(chunk)
    end
    result
  end

  def write_without_tags(chunk)
    records = []
    chunk.msgpack_each { |record|
      record[@time_key] = Time.at(record[@time_key]) if @include_time_key
      records << record
    }
    operate(@collection, records)
  end

  def write_with_tags(chunk)
    collections = {}

    chunk.msgpack_each { |tag, record|
      record[@time_key] = Time.at(record[@time_key]) if @include_time_key
      (collections[tag] ||= []) << record
    }

    collections.each { |collection_name, records|
      operate(collection_name, records)
    }
  end

  def format_collection_name(collection_name)
    formatted = collection_name
    formatted = formatted.gsub(@remove_prefix_collection, '') if @remove_prefix_collection
    formatted = formatted.gsub(/(^\.+)|(\.+$)/, '')
    formatted = @collection if formatted.size == 0 # set default for nil tag
    formatted
  end

  private

  def get_or_create_collection(collection_name)
    collection_name = format_collection_name(collection_name)
    return @clients[collection_name] if @clients[collection_name]

    @db ||= Mongo::Connection.new(@host, @port).db(@database)
    if @db.collection_names.include?(collection_name)
      collection = @db.collection(collection_name)
      unless @argument[:capped] == collection.capped? # TODO: Verify capped configuration
        # raise Exception if old collection does not match lastest configuration
        raise ConfigError, "New configuration is different from existing collection"
      end
    else
      collection = @db.create_collection(collection_name, @argument)
    end
    @clients[collection_name] = collection
  end

  def operate(collection_name, records)
    get_or_create_collection(collection_name).insert(records)
  end
end


end
