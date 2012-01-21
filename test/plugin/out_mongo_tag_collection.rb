require 'test_helper'

class MongoTagCollectionTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_mongo'
  end

  CONFIG = %[
    type mongo
    database fluent
    tag_mapped
    remove_tag_prefix should.remove.
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
    }.configure(conf)
  end

  def test_configure
    d = create_driver(CONFIG)
    assert_equal(/^should\.remove\./, d.instance.instance_variable_get(:@remove_tag_prefix))
  end

  def emit_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    time
  end

  def test_write
    skip('BufferedOutputTestDriver should support emit arguments(chain and key)')

    d = create_driver(CONFIG)
    d.tag = 'mytag'
    t = emit_documents(d)
    mock(d.instance).operate('mytag', [{'a' => 1, d.instance.time_key => Time.at(t)},
                                       {'a' => 2, d.instance.time_key => Time.at(t)}])
    d.run
  end

  def test_remove_prefix_collection
    d = create_driver(CONFIG)
    assert_equal('prefix', d.instance.__send__(:format_collection_name, 'should.remove.prefix'))
    assert_equal('test', d.instance.__send__(:format_collection_name, '..test..'))
    assert_equal('test.foo', d.instance.__send__(:format_collection_name, '..test.foo.'))
  end
end
