$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'rr'
require 'test/unit'

if ENV['SIMPLE_COV']
  require 'simplecov'
  SimpleCov.start do 
    add_filter 'test/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

require 'test/unit'
require 'fluent/test'

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

module MongoTestHelper
  def self.cleanup_mongod_env
    Process.kill "TERM", @@pid
    Process.waitpid @@pid
    system("rm -rf #{MONGO_DB_PATH}")
  end

  def self.setup_mongod
    system("rm -rf #{MONGO_DB_PATH}")
    system("mkdir -p #{MONGO_DB_PATH}")

    @@mongod_port = unused_port
    @@pid = spawn(ENV['mongod'], "--port=#{@@mongod_port}", "--dbpath=#{MONGO_DB_PATH}")
    sleep 3
  end

  def self.teardown_mongod
    Mongo::Connection.new('localhost', @@mongod_port).drop_database(MONGO_DB_DB)
    cleanup_mongod_env
  end

  def self.mongod_port
    @@mongod_port
  end
end
