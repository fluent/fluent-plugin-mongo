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

  def configure(conf)
    super

    @nodes = parse_servers(conf['nodes'])
    @rs_argument = {}
    if name = conf['name']
      @rs_argument[:name] = conf['name']
    end
    if read = conf['read']
      @rs_argument[:read] = read.to_sym
    end
    if refresh_mode = conf['refresh_mode']
      @rs_argument[:refresh_mode] = refresh_mode.to_sym
    end
    if refresh_interval = conf['refresh_interval']
      @rs_argument[:refresh_interval] = refresh_interval
    end
  end

  private

  def operate(collection_name, records)
    collection = get_or_create_collection(collection_name)
    rescue_connection_failure do
      collection.insert(records)
    end
  end

  def parse_servers(servers)
    servers.split(',').map { |server|
      host, port = server.split(':')
      [host, Integer(port)]
    }
  end

  def get_connection
    Mongo::ReplSetConnection.new(*@nodes, @rs_argument).db(@database)
  end

  def rescue_connection_failure
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => e
      retries += 1
      raise e if retries > @num_retries
      sleep 0.5
      retry
    end
  end
end


end
