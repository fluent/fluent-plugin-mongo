# coding: utf-8
require "helper"
require "fluent/test/driver/output"
require "fluent/test/helpers"

class MongoOutputTest < ::Test::Unit::TestCase
  include Fluent::Test::Helpers

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

  def teardown_mongod(collection = collection_name)
    @client[collection].drop
  end

  def create_driver(conf=default_config)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::MongoOutput).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo
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
      type mongo
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
        type mongo
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
    assert_equal('${tag}', d.instance.collection)
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

  def get_documents(collection = collection_name)
    @client[collection].find.to_a.map {|e| e.delete('_id'); e}
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

  class WriteWithCollectionPlaceholder < self
    def setup
      @tag = 'custom'
      setup_mongod
    end

    def teardown
      teardown_mongod(@tag)
    end

    def test_write_with_collection_placeholder
      d = create_driver(%[
      type mongo
      database #{database_name}
      collection ${tag}
      include_time_key true
    ])
      d.run(default_tag: @tag) do
        emit_documents(d)
      end
      actual_documents = get_documents(@tag)
      time = event_time("2011-01-02 13:14:15 UTC")
      expected = [{'a' => 1, d.instance.inject_config.time_key => Time.at(time).localtime},
                  {'a' => 2, d.instance.inject_config.time_key => Time.at(time).localtime}]
      assert_equal(expected, actual_documents)
    end
  end

  def test_write_at_enable_tag
    d = create_driver(default_config + %[
      include_tag_key true
      include_time_key false
    ])
    d.run(default_tag: 'test') do
      emit_documents(d)
    end
    actual_documents = get_documents
    expected = [{'a' => 1, d.instance.inject_config.tag_key => 'test'},
                {'a' => 2, d.instance.inject_config.tag_key => 'test'}]
    assert_equal(expected, actual_documents)
  end

  def emit_invalid_documents(d)
    time = event_time("2011-01-02 13:14:15 UTC")
    d.feed(time, {'a' => 3, 'b' => "c", '$last' => '石動'})
    d.feed(time, {'a' => 4, 'b' => "d", 'first' => '菖蒲'.encode('EUC-JP').force_encoding('UTF-8')})
    time
  end

  def test_write_with_invalid_recoreds_with_keys_containing_dot_and_dollar
    d = create_driver(default_config + %[
      replace_dot_in_key_with _dot_
      replace_dollar_in_key_with _dollar_
    ])

    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: 'test') do
      d.feed(time, {
        "foo.bar1" => {
           "$foo$bar" => "baz"
        },
        "foo.bar2" => [
          {
            "$foo$bar" => "baz"
          }
        ],
             })
    end

    documents = get_documents
    expected = {"foo_dot_bar1" => {
                  "_dollar_foo$bar"=>"baz"
                },
                "foo_dot_bar2" => [
                  {
                    "_dollar_foo$bar"=>"baz"
                  },
                ], "time" => Time.at(time).localtime
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
