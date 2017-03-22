require 'fluent/plugin/out_mongo'

module Fluent::Plugin
  class MongoOutputReplset < MongoOutput
    Fluent::Plugin.register_output('mongo_replset', self)

    config_set_default :include_tag_key, false
    config_set_default :include_time_key, true

    desc "Nodes of replica set"
    config_param :nodes, :array, value_type: :string, :default => nil
    desc "Replica set name"
    config_param :replica_set, :string
    desc "Read from specified role"
    config_param :read, :string, :default => nil
    desc "Retry number"
    config_param :num_retries, :integer, :default => 60

    def configure(conf)
      super

      if replica_set = conf['replica_set']
        @client_options[:replica_set] = replica_set
      end
      if read = conf['read']
        @client_options[:read] = read.to_sym
      end

      log.debug "Setup replica set configuration: #{conf['replica_set']}"
    end

    def format(tag, time, record)
      super
    end

    def write(chunk)
      super
    end

    private

    def operate(client, records)
      rescue_connection_failure do
        super(client, records)
      end
    end

    def rescue_connection_failure
      retries = 0
      begin
        yield
      rescue Mongo::Error::OperationFailure => e
        retries += 1
        raise e if retries > @num_retries

        log.warn "Failed to operate to Replica Set. Try to retry: retry count = #{retries}"

        sleep 0.5
        retry
      end
    end
  end
end
