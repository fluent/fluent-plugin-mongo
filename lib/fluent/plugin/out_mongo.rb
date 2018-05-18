require 'mongo'
require 'msgpack'
require 'fluent/plugin/output'
require 'fluent/plugin/mongo_auth'
require 'fluent/plugin/logger_support'

module Fluent::Plugin
  class MongoOutput < Output
    Fluent::Plugin.register_output('mongo', self)

    helpers :event_emitter, :inject, :compat_parameters

    include Fluent::MongoAuthParams
    include Fluent::MongoAuth
    include Fluent::LoggerSupport

    DEFAULT_BUFFER_TYPE = "memory"

    config_set_default :include_tag_key, false
    config_set_default :include_time_key, true

    desc "MongoDB connection string"
    config_param :connection_string, :default => nil
    desc "MongoDB database"
    config_param :database, :string, :default => nil
    desc "MongoDB collection"
    config_param :collection, :string, default: 'untagged'
    desc "MongoDB host"
    config_param :host, :string, default: 'localhost'
    desc "MongoDB port"
    config_param :port, :integer, default: 27017
    desc "MongoDB write_concern"
    config_param :write_concern, :integer, default: nil
    desc "MongoDB journaled"
    config_param :journaled, :bool, default: false
    desc "Replace dot with specified string"
    config_param :replace_dot_in_key_with, :string, default: nil
    desc "Replace dollar with specified string"
    config_param :replace_dollar_in_key_with, :string, default: nil

    # tag mapping mode
    desc "Use tag_mapped mode"
    config_param :tag_mapped, :bool, default: false,
                 deprecated: "use '${tag}' placeholder in collection parameter."
    desc "Remove tag prefix"
    config_param :remove_tag_prefix, :string, default: nil,
                 deprecated: "use @label instead for event routing."

    # SSL connection
    config_param :ssl, :bool, default: false
    config_param :ssl_cert, :string, default: nil
    config_param :ssl_key, :string, default: nil
    config_param :ssl_key_pass_phrase, :string, default: nil, secret: true
    config_param :ssl_verify, :bool, default: false
    config_param :ssl_ca_cert, :string, default: nil

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      config_set_default :chunk_keys, ['tag']
    end

    attr_reader :client_options, :collection_options

    def initialize
      super

      @nodes = nil
      @client_options = {}
      @collection_options = {capped: false}
    end

    # Following limits are heuristic. BSON is sometimes bigger than MessagePack and JSON.
    LIMIT_BEFORE_v1_8 = 2 * 1024 * 1024  # 2MB = 4MB  / 2
    LIMIT_AFTER_v1_8 =  8 * 1024 * 1024  # 8MB = 16MB / 2

    def configure(conf)
      if conf.has_key?('buffer_chunk_limit')
        configured_chunk_limit_size = Fluent::Config.size_value(conf['buffer_chunk_limit'])
        estimated_limit_size = LIMIT_AFTER_v1_8
        estimated_limit_size_conf = '8m'
        if conf.has_key?('mongodb_smaller_bson_limit') && Fluent::Config.bool_value(conf['mongodb_smaller_bson_limit'])
          estimated_limit_size = LIMIT_BEFORE_v1_8
          estimated_limit_size_conf = '2m'
        end
        if configured_chunk_limit_size > estimated_limit_size
          log.warn ":buffer_chunk_limit(#{conf['buffer_chunk_limit']}) is large. Reset :buffer_chunk_limit with #{estimated_limit_size_conf}"
          conf['buffer_chunk_limit'] = estimated_limit_size_conf
        end
      else
        if conf.has_key?('mongodb_smaller_bson_limit') && Fluent::Config.bool_value(conf['mongodb_smaller_bson_limit'])
          conf['buffer_chunk_limit'] = '2m'
        else
          conf['buffer_chunk_limit'] = '8m'
        end
      end
      # 'config_set_default :include_time_key, true' is ignored in compat_parameters_convert so need manual setting
      if conf.elements('inject').empty?
        if conf.has_key?('include_time_key')
          if Fluent::Config.bool_value(conf['include_time_key']) && !conf.has_key?('time_key')
            conf['time_key'] = 'time'
          end
        else
          conf['time_key'] = 'time'
        end
      end

      compat_parameters_convert(conf, :inject)

      super

      if @connection_string.nil? && @database.nil?
        raise Fluent::ConfigError,  "connection_string or database parameter is required"
      end

      unless @ignore_invalid_record
        log.warn "Since v0.8, invalid record detection will be removed because mongo driver v2.x and API spec don't provide it. You may lose invalid records, so you should not send such records to mongo plugin"
      end

      if conf.has_key?('tag_mapped')
        log.warn "'tag_mapped' feature is replaced with built-in config placeholder. Please consider to use 'collection ${tag}'."
        @collection = '${tag}'
      end
      raise Fluent::ConfigError, "normal mode requires collection parameter" if !@tag_mapped and !conf.has_key?('collection')

      if conf.has_key?('capped')
        raise Fluent::ConfigError, "'capped_size' parameter is required on <store> of Mongo output" unless conf.has_key?('capped_size')
        @collection_options[:capped] = true
        @collection_options[:size] = Fluent::Config.size_value(conf['capped_size'])
        @collection_options[:max] = Fluent::Config.size_value(conf['capped_max']) if conf.has_key?('capped_max')
      end

      if remove_tag_prefix = conf['remove_tag_prefix']
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_tag_prefix))
      end

      @client_options[:write] = {j: @journaled}
      @client_options[:write].merge!({w: @write_concern}) unless @write_concern.nil?
      @client_options[:ssl] = @ssl

      if @ssl
        @client_options[:ssl_cert] = @ssl_cert
        @client_options[:ssl_key] = @ssl_key
        @client_options[:ssl_key_pass_phrase] = @ssl_key_pass_phrase
        @client_options[:ssl_verify] = @ssl_verify
        @client_options[:ssl_ca_cert] = @ssl_ca_cert
      end
      @nodes = ["#{@host}:#{@port}"] if @nodes.nil?

      configure_logger(@mongo_log_level)

      log.debug "Setup mongo configuration: mode = #{@tag_mapped ? 'tag mapped' : 'normal'}"
    end

    def start
      @client = client
      @client = authenticate(@client)
      @collections = {}
      super
    end

    def shutdown
      @client.close
      super
    end

    def formatted_to_msgpack_binary
      true
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      collection_name = extract_placeholders(@collection, chunk.metadata)
      operate(format_collection_name(collection_name), collect_records(chunk))
    end

    private

    def client
      if @connection_string
        Mongo::Client.new(@connection_string)
      else
        @client_options[:database] = @database
        @client_options[:user] = @user if @user
        @client_options[:password] = @password if @password
        Mongo::Client.new(@nodes, @client_options)
      end
    end

    def collect_records(chunk)
      records = []
      time_key = @inject_config.time_key if @inject_config
      tag = chunk.metadata.tag
      chunk.msgpack_each {|time, record|
        record = inject_values_to_record(tag, time, record)
        # MongoDB uses BSON's Date for time.
        record[time_key] = Time.at(time || record[time_key]) if time_key
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

    def list_collections_enabled?
      @client.cluster.next_primary(false).features.list_collections_enabled?
    end

    def collection_exists?(name)
      if list_collections_enabled?
        r = @client.database.command(
          { :listCollections => 1, :filter => { :name => name } }
        ).first
        r[:ok] && r[:cursor][:firstBatch].size == 1
      else
        @client.database.collection_names.include?(name)
      end
    end

    def get_collection(name, options)
      return @client[name] if @collections[name]

      unless collection_exists?(name)
        log.trace "Create collection #{name} with options #{options}"
        @client[name, options].create
      end
      @collections[name] = true
      @client[name]
    end

    def forget_collection(name)
      @collections.delete(name)
    end

    def operate(collection, records)
      begin
        if @replace_dot_in_key_with
          records.map! do |r|
            replace_key_of_hash(r, ".", @replace_dot_in_key_with)
          end
        end
        if @replace_dollar_in_key_with
          records.map! do |r|
            replace_key_of_hash(r, /^\$/, @replace_dollar_in_key_with)
          end
        end

        get_collection(collection, @collection_options).insert_many(records)
      rescue Mongo::Error::BulkWriteError => e
        log.warn "#{records.size - e.result["n_inserted"]} documents are not inserted. Maybe these documents are invalid as a BSON."
        forget_collection(collection)
      rescue ArgumentError => e
        log.warn e
      end
      records
    end

    def replace_key_of_hash(hash_or_array, pattern, replacement)
      case hash_or_array
      when Array
        hash_or_array.map do |elm|
          replace_key_of_hash(elm, pattern, replacement)
        end
      when Hash
        result = Hash.new
        hash_or_array.each_pair do |k, v|
          k = k.gsub(pattern, replacement)

          if v.is_a?(Hash) || v.is_a?(Array)
            result[k] = replace_key_of_hash(v, pattern, replacement)
          else
            result[k] = v
          end
        end
        result
      else
        hash_or_array
      end
    end
  end
end
