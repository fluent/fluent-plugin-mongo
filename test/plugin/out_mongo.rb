require 'test_helper'

class MongoOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_mongo'
  end

  CONFIG = %[
    type mongo
    database fluent
    collection test
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutput) {
      def start
        super
      end

      def shutdown
        super
      end

      def operate(collection, records)
        [format_collection_name(collection), records]
      end

      def get_or_create_collection(collection_name)
        collection_name
      end

      def mongod_version
        "1.6.0"
      end
    }.configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo
      database fluent_test
      collection test_collection

      host fluenter
      port 27018

      capped
      capped_size 100
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('fluenter', d.instance.host)
    assert_equal(27018, d.instance.port)
    assert_equal({:capped => true, :size => 100}, d.instance.collection_options)
    assert_equal(33554432, d.instance.instance_variable_get(:@buffer).buffer_chunk_limit)
    # buffer_chunk_limit moved from configure to start
    # I will move this test to correct space after BufferedOutputTestDriver supports start method invoking
    # assert_equal(Fluent::MongoOutput::LIMIT_BEFORE_v1_8, d.instance.instance_variable_get(:@buffer).buffer_chunk_limit)
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    d.expect_format([time, {'a' => 1, d.instance.time_key => time}].to_msgpack)
    d.expect_format([time, {'a' => 2, d.instance.time_key => time}].to_msgpack)

    d.run
  end

  def emit_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    time
  end

  def test_write
    d = create_driver
    t = emit_documents(d)

    collection_name, documents = d.run
    assert_equal([{'a' => 1, d.instance.time_key => Time.at(t)},
                  {'a' => 2, d.instance.time_key => Time.at(t)}], documents)
    assert_equal('test', collection_name)
  end

  def test_write_with_invalid_recoreds
    skip('Implement this test using BSON directory later')
  end

  def test_write_at_enable_tag
    d = create_driver(CONFIG + %[
      include_tag_key true
      include_time_key false
    ])
    t = emit_documents(d)

    collection_name, documents = d.run
    assert_equal([{'a' => 1, d.instance.tag_key => 'test'},
                  {'a' => 2, d.instance.tag_key => 'test'}], documents)
    assert_equal('test', collection_name)
  end
end
