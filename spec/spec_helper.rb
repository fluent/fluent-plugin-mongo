$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'time'
require 'rspec'

if ENV['SIMPLE_COV']
  require 'simplecov'
  SimpleCov.start do 
    add_filter 'spec/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

require 'fluentd'
require 'fluentd/plugin_spec_helper'
require 'tools/rs_test_helper'

# for testing

def unused_port
  s = TCPServer.open(0)
  port = s.addr[1]
  s.close
  port
end

# for MongoDB

require 'mongo'

MONGO_DB_DB = 'fluent_test'
MONGO_DB_PATH = File.join(File.dirname(__FILE__), 'plugin', 'data')

module MongoTestHelpers
  def mongod_port
    @mongod_port
  end

  def cleanup_mongod_env
    system("killall mongod")
    system("rm -rf #{MONGO_DB_PATH}")
    system("mkdir -p #{MONGO_DB_PATH}")
  end

  def setup_mongod
    cleanup_mongod_env

    @mongod_port = unused_port
    @pid = spawn(ENV['mongod'], "--port=#{@mongod_port}", "--dbpath=#{MONGO_DB_PATH}")
    sleep 3

    @launched = true
  end

  def teardown_mongod
    if @launched
      Mongo::Connection.new('localhost', @mongod_port).drop_database(MONGO_DB_DB)
    end
  end
end

Rspec.configure do |c|
  c.include MongoTestHelpers, :mongod => true
  c.include ReplSetHelpers, :replset => true
end

Fluentd.setup!
