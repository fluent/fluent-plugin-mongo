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

  CONFIG_WITH_MAPPING = %[
    type mongo
    database fluent
    collection test

    tag_collection_mapping true
    remove_prefix_collection should.remove.
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutput) {
      def start
        super
      end

      def shutdown
        super
      end

      def operate(collection_name, records)
        [format_collection_name(collection_name), records]
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
    assert_equal({:capped => true, :size => 100}, d.instance.argument)
  end

  def test_configure_tag_collection_mapping
    d = create_driver(CONFIG_WITH_MAPPING)
    assert_equal(true, d.instance.instance_variable_get(:@tag_collection_mapping))
    assert_equal(/^should\.remove\./, d.instance.instance_variable_get(:@remove_prefix_collection))
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    d.expect_format({'a' => 1, d.instance.time_key => time}.to_msgpack)
    d.expect_format({'a' => 2, d.instance.time_key => time}.to_msgpack)

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

  def test_write_with_tag_collection_mapping
    d = create_driver(CONFIG_WITH_MAPPING)
    d.tag = 'mytag'
    t = emit_documents(d)
    mock(d.instance).operate('mytag', [{'a' => 1, d.instance.time_key => Time.at(t)},
                                       {'a' => 2, d.instance.time_key => Time.at(t)}])
    d.run
  end

  def test_remove_prefix_collection
    d = create_driver(CONFIG_WITH_MAPPING)
    assert_equal('prefix', d.instance.format_collection_name('should.remove.prefix'))
    assert_equal('test', d.instance.format_collection_name('..test..'))
    assert_equal('test.foo', d.instance.format_collection_name('..test.foo.'))
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