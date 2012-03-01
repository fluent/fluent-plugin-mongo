require 'fluent/plugin/out_mongo'

module Fluent


class MongoOutputReplset < MongoOutput
  Fluent::Plugin.register_output('mongo_replset', self)

  config_param :nodes, :string
  config_param :name, :string, :default => nil
  config_param :read, :string, :default => nil
  config_param :refresh_mode, :string, :default => nil
  config_param :refresh_interval, :integer, :default => nil
  config_param :num_retries, :integer, :default => 60

  # disable single node configuration
  config_param :host, :string, :default => nil
  config_param :port, :integer, :default => nil

  def configure(conf)
    super

    @nodes = parse_nodes(conf['nodes'])
    if name = conf['name']
      @connection_options[:name] = conf['name']
    end
    if read = conf['read']
      @connection_options[:read] = read.to_sym
    end
    if refresh_mode = conf['refresh_mode']
      @connection_options[:refresh_mode] = refresh_mode.to_sym
    end
    if refresh_interval = conf['refresh_interval']
      @connection_options[:refresh_interval] = refresh_interval
    end

    $log.debug "Setup replica set configuration: nodes = #{conf['nodes']}"
  end

  private

  def operate(collection, records)
    rescue_connection_failure do
      super(collection, records)
    end
  end

  def parse_nodes(nodes)
    nodes.split(',')
  end

  def get_connection
    db = Mongo::ReplSetConnection.new(@nodes, @connection_options).db(@database)
    authenticate(db)
  end

  def rescue_connection_failure
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => e
      retries += 1
      raise e if retries > @num_retries

      $log.warn "Failed to connect to Replica Set. Try to retry: retry number = #{retries}"
      sleep 0.5
      retry
    end
  end
end


end
