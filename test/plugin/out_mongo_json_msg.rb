require 'tools/rs_test_helper'

class MongoJSONTest < Test::Unit::TestCase
  include MongoTestHelper

  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_mongo'

    setup_mongod
  end

  def teardown
    @db.collection(collection_name).drop
    teardown_mongod
  end

  def collection_name
    'test'
  end

  def default_config
    %[
      type mongo
      database #{MONGO_DB_DB}
      collection #{collection_name}
      include_time_key true # TestDriver ignore config_set_default?
    ]
  end

  def create_driver(conf = default_config)
    conf = conf + %[
      json_msg
      port #{@@mongod_port}
    ]
    @db = Mongo::MongoClient.new('localhost', @@mongod_port).db(MONGO_DB_DB)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutput).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo
      database fluent_test
      collection test_collection
      json_msg
      capped
      capped_size 100
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(@@mongod_port, d.instance.port)
    assert_equal({:capped => true, :size => 100}, d.instance.collection_options)
    assert_equal({:ssl => false, :j => false}, d.instance.connection_options)
    # buffer_chunk_limit moved from configure to start
    # I will move this test to correct space after BufferedOutputTestDriver supports start method invoking
    # assert_equal(Fluent::MongoOutput::LIMIT_BEFORE_v1_8, d.instance.instance_variable_get(:@buffer).buffer_chunk_limit)
  end

  def emit_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'msg' => '{"test": 1}'}, time)
    d.emit({'msg' => '{"test": 2}'}, time)
    time
  end

  def get_documents
    @db.collection(collection_name).find().to_a.map { |e| e.delete('_id'); e }
  end

  def test_write
    d = create_driver(%[
      type mongo
      database fluent_test
      collection test
      json_msg
      capped
      capped_size 100
    ])

    t = emit_documents(d)
    d.run

    documents = get_documents.map { |e| e['msg'] }
    assert_equal(1, documents[0]['test'])
    assert_equal(2, documents.size)
  end

end
