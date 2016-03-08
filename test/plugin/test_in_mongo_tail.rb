require "helper"

class MongoTailInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    setup_mongod
  end

  def teardown
    teardown_mongod
  end

  def collection_name
    'test'
  end

  def database_name
    'fluent_test'
  end

  def port
    27017
  end

  def default_config
    %[
      type mongo_tail
      database test
      collection log
      tag_key tag
      time_key time
      id_store_file /tmp/fluent_mongo_last_id
    ]
  end

  def setup_mongod
    options = {}
    options[:database] = database_name
    @client = ::Mongo::Client.new(["localhost:#{port}"], options)
  end

  def teardown_mongod
    @client[collection_name].drop
  end

  def create_driver(conf=default_config)
    Fluent::Test::InputTestDriver.new(Fluent::MongoTailInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal('localhost', d.instance.host)
    assert_equal(27017, d.instance.port)
    assert_equal('test', d.instance.database)
    assert_equal('log', d.instance.collection)
    assert_equal('tag', d.instance.tag_key)
    assert_equal('time', d.instance.time_key)
    assert_equal('/tmp/fluent_mongo_last_id', d.instance.id_store_file)
  end

  def test_configure_with_logger_conf
    d = create_driver(default_config + %[
      mongo_log_level error
    ])

    expected = "error"
    assert_equal(expected, d.instance.mongo_log_level)
  end

  class MongoAuthenticateTest < self
    require 'fluent/plugin/mongo_auth'
    include ::Fluent::MongoAuth

    def setup_mongod
      options = {}
      options[:database] = database_name
      @client = ::Mongo::Client.new(["localhost:#{port}"], options)
      @client.database.users.create('fluent', password: 'password',
                                    roles: [Mongo::Auth::Roles::READ_WRITE])
    end

    def teardown_mongod
      @client[collection_name].drop
      @client.database.users.remove('fluent')
    end

    def test_authenticate
      d = create_driver(default_config + %[
        user fluent
        password password
      ])

      assert authenticate(@client)
    end
  end
end
