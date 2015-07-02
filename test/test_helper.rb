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
  @@setup_count = 0

  def cleanup_mongod_env
    system("killall mongod")
    system("rm -rf #{MONGO_DB_PATH}")
    system("mkdir -p #{MONGO_DB_PATH}")
  end

  def setup_mongod
    unless defined?(@@current_mongo_test_class) and @@current_mongo_test_class == self.class
      cleanup_mongod_env

      @@current_mongo_test_class = self.class
      @@mongod_port = unused_port
      @@pid = spawn(ENV['mongod'], "--port=#{@@mongod_port}", "--dbpath=#{MONGO_DB_PATH}")
      sleep 3
    end

    @@setup_count += 1;
  end

  def teardown_mongod
    if defined?(@@current_mongo_test_class)
      Mongo::Connection.new('localhost', @@mongod_port).drop_database(MONGO_DB_DB)
    end
    if @@setup_count == self.class.methods.size
      cleanup_mongod_env
    end
  end
end
