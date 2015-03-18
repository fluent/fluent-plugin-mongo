require 'test_helper'

class MongoTailInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/in_mongo_tail'
  end

  CONFIG = %[
    type mongo_tail
    database test
    collection log
    tag_key tag
    time_key time
    id_store_file /tmp/fluent_mongo_last_id
    id_store_collection test_last_id
  ]

  def create_driver(conf = CONFIG)
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
    assert_equal('test_last_id', d.instance.id_store_collection)
  end
  
  def test_url_configration
    config = %[
    type mongo_tail
    url mongodb://localhost:27017/test
    collection log
    tag_key tag
    time_key time
    id_store_file /tmp/fluent_mongo_last_id
    ]
    
    d = create_driver(config)
    assert_equal("mongodb://localhost:27017/test", d.instance.url)
    assert_nil(d.instance.database)
    assert_equal('log', d.instance.collection)
    assert_equal('tag', d.instance.tag_key)
    assert_equal('time', d.instance.time_key)
    assert_equal('/tmp/fluent_mongo_last_id', d.instance.id_store_file)
  end

  def test_url_and_database_can_not_exist
    config = %[
    type mongo_tail
    url mongodb://localhost:27017/test
    database test2
    collection log
    tag_key tag
    time_key time
    id_store_file /tmp/fluent_mongo_last_id
    ]

    assert_raises Fluent::ConfigError do
      create_driver(config)
    end
  end

  def test_emit
    # TODO: write actual code
  end
end
