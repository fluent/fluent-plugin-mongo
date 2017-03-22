# coding: utf-8
require "helper"

class MongoOutputTest < ::Test::Unit::TestCase
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
      type mongo
      database #{database_name}
      collection #{collection_name}
      include_time_key true
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

  def create_driver(conf=default_config, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutput, tag).configure(conf, true)
  end

  def test_configure
    d = create_driver(%[
      @type mongo
      database fluent_test
      collection test_collection

      capped
      capped_size 100
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(port, d.instance.port)
    assert_equal({capped: true, size: 100}, d.instance.collection_options)
    assert_equal({ssl: false, write: {j: false}}, d.instance.client_options)
    assert_nil d.instance.connection_string
  end

  def test_configure_with_connection_string
    d = create_driver(%[
      @type mongo
      connection_string mongodb://localhost/fluent_test
      collection test_collection
      capped
      capped_size 100
    ])
    assert_equal('mongodb://localhost/fluent_test', d.instance.connection_string)
    assert_nil d.instance.database
  end

  def test_configure_without_connection_string_or_database
    assert_raise Fluent::ConfigError do
      d = create_driver(%[
        @type mongo
        collection test_collection
        capped
        capped_size 100
      ])
    end
  end

  def test_configure_with_ssl
    conf = default_config + %[
      ssl true
    ]
    d = create_driver(conf)
    expected = {
      write: {
        j: false,
      },
      ssl: true,
      ssl_cert: nil,
      ssl_key: nil,
      ssl_key_pass_phrase: nil,
      ssl_verify: false,
      ssl_ca_cert: nil,
    }
    assert_equal(expected, d.instance.client_options)
  end

  def test_configure_with_tag_mapped
    conf = default_config + %[
      tag_mapped true
      remove_tag_prefix raw.
    ]
    d = create_driver(conf)
    assert_true(d.instance.tag_mapped)
    assert_equal(/^raw\./, d.instance.remove_tag_prefix)
  end

  def test_configure_with_write_concern
    d = create_driver(default_config + %[
      write_concern 2
    ])

    expected = {
      ssl: false,
      write: {
        j: false,
        w: 2,
      },
    }
    assert_equal(expected, d.instance.client_options)
  end

  def test_configure_with_journaled
    d = create_driver(default_config + %[
      journaled true
    ])

    expected = {
      ssl: false,
      write: {
        j: true,
      },
    }
    assert_equal(expected, d.instance.client_options)
  end

  def test_configure_with_logger_conf
    d = create_driver(default_config + %[
      mongo_log_level fatal
    ])

    expected = "fatal"
    assert_equal(expected, d.instance.mongo_log_level)
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

  def test_write_at_enable_tag
    d = create_driver(default_config + %[
      include_tag_key true
      include_time_key false
    ])
    t = emit_documents(d)

    d.run
    actual_documents = get_documents
    expected = [{'a' => 1, d.instance.tag_key => 'test'},
                {'a' => 2, d.instance.tag_key => 'test'}]
    assert_equal(expected, actual_documents)
  end

  def emit_invalid_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 3, 'b' => "c", '$last' => '石動'}, time)
    d.emit({'a' => 4, 'b' => "d", 'first' => '菖蒲'.encode('EUC-JP').force_encoding('UTF-8')}, time)
    time
  end

  def test_write_with_invalid_recoreds_with_keys_containing_dot_and_dollar
    d = create_driver(default_config + %[
      replace_dot_in_key_with _dot_
      replace_dollar_in_key_with _dollar_
    ])

    original_time = "2016-02-01 13:14:15 UTC"
    time = Time.parse(original_time).to_i
    d.emit({
      "foo.bar1" => {
        "$foo$bar" => "baz"
      },
      "foo.bar2" => [
        {
          "$foo$bar" => "baz"
        }
      ],
    }, time)
    d.run

    documents = get_documents
    expected = {"foo_dot_bar1" => {
                  "_dollar_foo$bar"=>"baz"
                },
                "foo_dot_bar2" => [
                  {
                    "_dollar_foo$bar"=>"baz"
                  },
                ], "time" => Time.parse(original_time)
               }
    assert_equal(1, documents.size)
    assert_equal(expected, documents[0])
  end

  class WithAuthenticateTest < self
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

    def test_write_with_authenticate
      d = create_driver(default_config + %[
        user fluent
        password password
      ])
      t = emit_documents(d)

      d.run
      actual_documents = get_documents
      time = Time.parse("2011-01-02 13:14:15 UTC")
      expected = [{'a' => 1, d.instance.time_key => time},
                  {'a' => 2, d.instance.time_key => time}]
      assert_equal(expected, actual_documents)
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
