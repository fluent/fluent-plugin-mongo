require "helper"
require "fluent/test/driver/input"
require "fluent/test/helpers"
require "timecop"

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
    Fluent::Test::Driver::Input.new(Fluent::Plugin::MongoTailInput).configure(conf)
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

  class TailInputTest < self
    include Fluent::Test::Helpers

    def setup_mongod
      options = {}
      options[:database] = database_name
      @client = ::Mongo::Client.new(["localhost:#{port}"], options)
      @time = Time.now
      Timecop.freeze(@time)
    end

    def teardown_mongod
      @client[collection_name].drop
      Timecop.return
    end

    def test_emit
      d = create_driver(%[
        @type mongo_tail
        database #{database_name}
        collection #{collection_name}
        tag input.mongo
        time_key time
      ])
      d.run(expect_records: 1, timeout: 5) do
        @client[collection_name].insert_one({message: "test"})
      end
      events = d.events
      assert_equal "input.mongo", events[0][0]
      assert_equal event_time(@time.to_s), events[0][1]
      assert_equal "test", events[0][2]["message"]
    end

    def test_emit_with_tag_time_keys
      d = create_driver(%[
        @type mongo_tail
        database #{database_name}
        collection #{collection_name}
        tag input.mongo
        tag_key tag
        time_key time
      ])
      d.run(expect_records: 1, timeout: 5) do
        @client[collection_name].insert_one({message: "test", tag: "user.defined", time: Fluent::Engine.now})
      end
      events = d.events
      assert_equal "user.defined", events[0][0]
      assert_equal event_time(@time.to_s), events[0][1]
      assert_equal "test", events[0][2]["message"]
    end

    def test_emit_after_last_id
      d = create_driver(%[
        @type mongo_tail
        database #{database_name}
        collection #{collection_name}
        tag input.mongo.last_id
        time_key time
      ])
      @client[collection_name].insert_one({message: "can't obtain"})

      d.run(expect_records: 1, timeout: 5) do
        @client[collection_name].insert_one({message: "can obtain"})
      end
      events = d.events
      assert_equal 1, events.size
      assert_equal "input.mongo.last_id", events[0][0]
      assert_equal event_time(@time.to_s), events[0][1]
      assert_equal "can obtain", events[0][2]["message"]
    end
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
