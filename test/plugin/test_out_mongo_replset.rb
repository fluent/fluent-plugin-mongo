require "helper"

class MongoReplsetOutputTest < ::Test::Unit::TestCase
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

  def port
    27018
  end

  def default_config
    %[
      type mongo
      port 27018
      database #{database_name}
      collection #{collection_name}
      include_time_key true
      replica_set rs0
    ]
  end

  def create_driver(conf=default_config, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutputReplset, tag).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo
      port 27018
      database fluent_test
      collection test_collection

      replica_set rs0
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(port, d.instance.port)
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
      @client = ::Mongo::Client.new(["localhost:#{port}"], options)
    end

    def teardown_mongod
      @client[collection_name].drop
    end

    def get_documents
      @client[collection_name].find.to_a.map {|e| e.delete('_id'); e}
    end

    def emit_documents(d)
      time = Time.parse("2011-01-02 13:14:15 UTC").to_i
      d.emit({'a' => 1}, time)
      d.emit({'a' => 2}, time)
      time
    end

    def test_format
      d = create_driver

      time = Time.parse("2011-01-02 13:14:15 UTC").to_i
      d.emit({'a' => 1}, time)
      d.emit({'a' => 2}, time)
      d.expect_format([time, {'a' => 1, d.instance.time_key => time}].to_msgpack)
      d.expect_format([time, {'a' => 2, d.instance.time_key => time}].to_msgpack)
      d.run

      documents = get_documents
      assert_equal(2, documents.size)
    end

    def test_write
      d = create_driver
      t = emit_documents(d)

      d.run
      actual_documents = get_documents
      time = Time.parse("2011-01-02 13:14:15 UTC")
      expected = [{'a' => 1, d.instance.time_key => time},
                  {'a' => 2, d.instance.time_key => time}]
      assert_equal(expected, actual_documents)
    end
  end
end
