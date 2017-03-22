require "helper"
require "fluent/test/driver/output"
require "fluent/test/helpers"
require 'fluent/mixin' # for TimeFormatter

class MongoReplsetOutputTest < ::Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  def teardown
  end

  def collection_name
    'test'
  end

  def database_name
    'fluent_test'
  end

  def nodes
    ["localhost:#{port}"]
  end

  def port
    27018
  end

  def default_config
    %[
      @type mongo_replset
      nodes localhost:27018
      database #{database_name}
      collection #{collection_name}
      include_time_key true
      replica_set rs0
    ]
  end

  def create_driver(conf=default_config)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::MongoOutputReplset).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      @type mongo_replset
      port 27018
      database fluent_test
      collection test_collection
      replica_set rs0
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(27018, d.instance.port)
    assert_equal({replica_set: 'rs0', :ssl=>false, :write=>{:j=>false}},
                 d.instance.client_options)
  end

  def test_configure_with_nodes
    d = create_driver(%[
      @type mongo_replset
      nodes localhost:27018,localhost:27019
      database fluent_test
      collection test_collection
      replica_set rs0
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal(['localhost:27018', 'localhost:27019'], d.instance.nodes)
    assert_equal({replica_set: 'rs0', :ssl=>false, :write=>{:j=>false}},
                 d.instance.client_options)
  end

  def test_configure_with_logger_conf
    d = create_driver(default_config + %[
      mongo_log_level fatal
    ])

    expected = "fatal"
    assert_equal(expected, d.instance.mongo_log_level)
  end

  class ReplisetWriteTest < self
    def setup
      omit("Replica set setup is too hard in CI.") if ENV['CI']

      setup_mongod
    end

    def teardown
      omit("Replica set setup is too hard in CI.") if ENV['CI']

      teardown_mongod
    end

    def setup_mongod
      options = {}
      options[:database] = database_name
      @client = ::Mongo::Client.new(nodes, options)
    end

    def teardown_mongod
      @client[collection_name].drop
    end

    def get_documents
      @client[collection_name].find.to_a.map {|e| e.delete('_id'); e}
    end

    def emit_documents(d)
      time = event_time("2011-01-02 13:14:15 UTC")
      d.feed(time, {'a' => 1})
      d.feed(time, {'a' => 2})
      time
    end

    def test_format
      d = create_driver

      time = event_time("2011-01-02 13:14:15 UTC")
      d.run(default_tag: 'test') do
        d.feed(time, {'a' => 1})
        d.feed(time, {'a' => 2})
      end
      assert_equal([time, {'a' => 1}].to_msgpack, d.formatted[0])
      assert_equal([time, {'a' => 2}].to_msgpack, d.formatted[1])
      assert_equal(2, d.formatted.size)
    end

    def test_write
      d = create_driver
      d.run(default_tag: 'test') do
        emit_documents(d)
      end
      actual_documents = get_documents
      time = event_time("2011-01-02 13:14:15 UTC")
      expected = [{'a' => 1, d.instance.inject_config.time_key => Time.at(time).localtime},
                  {'a' => 2, d.instance.inject_config.time_key => Time.at(time).localtime}]
      assert_equal(expected, actual_documents)
    end
  end
end
