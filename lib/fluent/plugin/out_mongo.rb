module Fluent


class MongoOutput < BufferedOutput
  Fluent::Plugin.register_output('mongo', self)

  include SetTagKeyMixin
  config_set_default :include_tag_key, false

  include SetTimeKeyMixin
  config_set_default :include_time_key, true

  config_param :database, :string
  config_param :collection, :string, :default => 'untagged'
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 27017
  config_param :user, :string
  config_param :password, :string
  config_param :ignore_invalid_record, :bool, :default => false
  config_param :safe, :bool, :default => true

  # tag mapping mode
  config_param :tag_mapped, :bool, :default => false
  config_param :remove_tag_prefix, :string, :default => nil

  attr_reader :collection_options

  def initialize
    super
    require 'mongo'
    require 'fluent/plugin/mongo_ext'
    require 'msgpack'

    @clients = {}
    @connection_options = {}
    @collection_options = {:capped => false}
  end

  def configure(conf)
    super

    @tag_mapped = true if conf.has_key?('tag_mapped')
    raise ConfigError, "normal mode requires collection parameter" if !@tag_mapped and !conf.has_key?('collection')

    if remove_tag_prefix = conf['remove_tag_prefix']
      @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
    end

    # capped configuration
    if conf.has_key?('capped')
      raise ConfigError, "'capped_size' parameter is required on <store> of Mongo output" unless conf.has_key?('capped_size')
      @collection_options[:capped] = true
      @collection_options[:size] = Config.size_value(conf['capped_size'])
      @collection_options[:max] = Config.size_value(conf['capped_max']) if conf.has_key?('capped_max')
    end

    if @buffer.respond_to?(:buffer_chunk_limit)
      @buffer.buffer_chunk_limit = available_buffer_chunk_limit
    else
      $log.warn "#{Fluent::VERSION} does not have :buffer_chunk_limit. Be careful when insert large documents to MongoDB"
    end

    @connection_options[:safe] = @safe

    # MongoDB uses BSON's Date for time.
    def @timef.format_nocache(time)
      time
    end

    $log.debug "Setup mongo configuration: mode = #{@tag_mapped ? 'tag mapped' : 'normal'}"
  end

  def start
    super
  end

  def shutdown
    # Mongo::Connection checks alive or closed myself
    @clients.values.each { |client| client.db.connection.close }
    super
  end

  def format(tag, time, record)
    [time, record].to_msgpack
  end

  def emit(tag, es, chain)
    # TODO: Should replacement using eval in configure?
    if @tag_mapped
      super(tag, es, chain, tag)
    else
      super(tag, es, chain)
    end
  end

  def write(chunk)
    # TODO: See emit comment
    collection_name = @tag_mapped ? chunk.key : @collection
    operate(get_or_create_collection(collection_name), collect_records(chunk))
  end

  private

  INSERT_ARGUMENT = {:collect_on_error => true}
  BROKEN_DATA_KEY = '__broken_data'

  def operate(collection, records)
    begin
      record_ids, error_records = collection.insert(records, INSERT_ARGUMENT)
      if !@ignore_invalid_record and error_records.size > 0
        operate_invalid_records(collection, error_records)
      end
    rescue Mongo::OperationFailure => e
      # Probably, all records of _records_ are broken...
      if e.error_code == 13066  # 13066 means "Message contains no documents"
        operate_invalid_records(collection, records) unless @ignore_invalid_record
      else
        raise e
      end
    end
    records
  end

  def operate_invalid_records(collection, records)
    converted_records = records.map { |record|
      new_record = {}
      new_record[@tag_key] = record.delete(@tag_key) if @include_tag_key
      new_record[@time_key] = record.delete(@time_key)
      new_record[BROKEN_DATA_KEY] = Marshal.dump(record) # Should use BSON::ByteBuffer
      new_record
    }
    collection.insert(converted_records) # Should create another collection like name_broken?
  end

  def collect_records(chunk)
    records = []
    chunk.msgpack_each { |time, record|
      record[@time_key] = Time.at(time || record[@time_key]) if @include_time_key
      records << record
    }
    records
  end

  FORMAT_COLLECTION_NAME_RE = /(^\.+)|(\.+$)/

  def format_collection_name(collection_name)
    formatted = collection_name
    formatted = formatted.gsub(@remove_tag_prefix, '') if @remove_tag_prefix
    formatted = formatted.gsub(FORMAT_COLLECTION_NAME_RE, '')
    formatted = @collection if formatted.size == 0 # set default for nil tag
    formatted
  end

  def get_or_create_collection(collection_name)
    collection_name = format_collection_name(collection_name)
    return @clients[collection_name] if @clients[collection_name]

    @db ||= get_connection
    if @db.collection_names.include?(collection_name)
      collection = @db.collection(collection_name)
      unless @collection_options[:capped] == collection.capped? # TODO: Verify capped configuration
        # raise Exception if old collection does not match lastest configuration
        raise ConfigError, "New configuration is different from existing collection"
      end
    else
      collection = @db.create_collection(collection_name, @collection_options)
    end

    @clients[collection_name] = collection
  end

  def get_connection
    db = Mongo::Connection.new(@host, @port, @connection_options).db(@database)
    return authenticate(db)
  end

  # Following limits are heuristic. BSON is sometimes bigger than MessagePack and JSON.
  LIMIT_BEFORE_v1_8 = 2 * 1024 * 1024  # 2MB  = 4MB  / 2
  LIMIT_AFTER_v1_8 = 10 * 1024 * 1024  # 10MB = 16MB / 2 + alpha

  def available_buffer_chunk_limit
    begin
      limit = mongod_version >= "1.8.0" ? LIMIT_AFTER_v1_8 : LIMIT_BEFORE_v1_8  # TODO: each version comparison
    rescue Mongo::ConnectionFailure => e
      $log.warn "mongo connection failed, set #{LIMIT_BEFORE_v1_8} to chunk limit"
      limit = LIMIT_BEFORE_v1_8
    rescue Exception => e
      $log.warn "mongo unknown error #{e}, set #{LIMIT_BEFORE_v1_8} to chunk limit"
      limit = LIMIT_BEFORE_v1_8
    end

    if @buffer.buffer_chunk_limit > limit
      $log.warn ":buffer_chunk_limit(#{@buffer.buffer_chunk_limit}) is large. Reset :buffer_chunk_limit with #{limit}"
      limit
    else
      @buffer.buffer_chunk_limit
    end
  end

  def mongod_version
    db = Mongo::Connection.new(@host, @port).db('admin')
    authenticate(db)
    db.command('serverStatus' => 1)['version']
  end

  def authenticate(db)
    unless @user.nil? || @password.nil?
      begin
        db.authenticate(@user, @password)
      rescue Mongo::AuthenticationError => e
        $log.fatal e
        exit!
      end
    end
    return db
  end

end


end
