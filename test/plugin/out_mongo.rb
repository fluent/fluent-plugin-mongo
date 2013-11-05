# -*- coding: utf-8 -*-
require 'tools/rs_test_helper'

class MongoOutputTest < Test::Unit::TestCase
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
    ]
  end

  def create_driver(conf = default_config)
    conf = conf + %[
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

      capped
      capped_size 100
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal('localhost', d.instance.host)
    assert_equal(@@mongod_port, d.instance.port)
    assert_equal({:capped => true, :size => 100}, d.instance.collection_options)
    assert(d.instance.connection_options.empty?)
    # buffer_chunk_limit moved from configure to start
    # I will move this test to correct space after BufferedOutputTestDriver supports start method invoking
    # assert_equal(Fluent::MongoOutput::LIMIT_BEFORE_v1_8, d.instance.instance_variable_get(:@buffer).buffer_chunk_limit)
  end

  def test_configure_with_write_concern
    d = create_driver(default_config + %[
      write_concern 2
    ])

    assert_equal({:w => 2}, d.instance.connection_options)
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    d.expect_format([time, {'a' => 1, d.instance.time_key => time}].to_msgpack)
    d.expect_format([time, {'a' => 2, d.instance.time_key => time}].to_msgpack)
    d.run

    assert_equal(2, @db.collection(collection_name).count)
  end

  def emit_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 1}, time)
    d.emit({'a' => 2}, time)
    time
  end

  def get_documents
    @db.collection(collection_name).find().to_a.map { |e| e.delete('_id'); e }
  end

  def test_write
    d = create_driver
    t = emit_documents(d)

    d.run
    documents = get_documents.map { |e| e['a'] }.sort
    assert_equal([1, 2], documents)
    assert_equal(2, documents.size)
  end

  def test_write_at_enable_tag
    d = create_driver(default_config + %[
      include_tag_key true
      include_time_key false
    ])
    t = emit_documents(d)

    d.run
    documents = get_documents.sort_by { |e| e['a'] }
    assert_equal([{'a' => 1, d.instance.tag_key => 'test'},
                  {'a' => 2, d.instance.tag_key => 'test'}], documents)
    assert_equal(2, documents.size)
  end

  def emit_invalid_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({'a' => 3, 'b' => "c", '$last' => '石動'}, time)
    d.emit({'a' => 4, 'b' => "d", 'first' => '菖蒲'.encode('EUC-JP').force_encoding('UTF-8')}, time)
    time
  end

  def test_write_with_invalid_recoreds
    d = create_driver
    t = emit_documents(d)
    t = emit_invalid_documents(d)

    d.run
    documents = get_documents
    assert_equal(4, documents.size)
    assert_equal([1, 2], documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort)
    assert_equal(2, documents.select { |e| e.has_key?(Fluent::MongoOutput::BROKEN_DATA_KEY)}.size)
    assert_equal([3, 4], @db.collection(collection_name).find({Fluent::MongoOutput::BROKEN_DATA_KEY => {'$exists' => true}}).map { |doc|
      Marshal.load(doc[Fluent::MongoOutput::BROKEN_DATA_KEY].to_s)['a']
    }.sort)
  end

  def test_write_with_invalid_recoreds_with_exclude_one_broken_fields
    d = create_driver(default_config + %[
      exclude_broken_fields a
    ])
    t = emit_documents(d)
    t = emit_invalid_documents(d)

    d.run
    documents = get_documents
    assert_equal(4, documents.size)
    assert_equal(2, documents.select { |e| e.has_key?(Fluent::MongoOutput::BROKEN_DATA_KEY) }.size)
    assert_equal([1, 2, 3, 4], documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort)
    assert_equal(0, documents.select { |e| e.has_key?('b') }.size)
  end

  def test_write_with_invalid_recoreds_with_exclude_two_broken_fields
    d = create_driver(default_config + %[
      exclude_broken_fields a,b
    ])
    t = emit_documents(d)
    t = emit_invalid_documents(d)

    d.run
    documents = get_documents
    assert_equal(4, documents.size)
    assert_equal(2, documents.select { |e| e.has_key?(Fluent::MongoOutput::BROKEN_DATA_KEY) }.size)
    assert_equal([1, 2, 3, 4], documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort)
    assert_equal(["c", "d"], documents.select { |e| e.has_key?('b') }.map { |e| e['b'] }.sort)
  end

  def test_write_with_invalid_recoreds_at_ignore
    d = create_driver(default_config + %[
      ignore_invalid_record true
    ])
    t = emit_documents(d)
    t = emit_invalid_documents(d)

    d.run
    documents = get_documents
    assert_equal(2, documents.size)
    assert_equal([1, 2], documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort)
    assert_equal(true, @db.collection(collection_name).find({Fluent::MongoOutput::BROKEN_DATA_KEY => {'$exists' => true}}).count.zero?)
  end

  def test_write_with_duplicate_key_error_at_ignore
    d = create_driver(default_config + %[
      ignore_duplicate_key_error true
    ])

    @db.collection(collection_name).ensure_index({a: 1}, {unique: true, dropDups: true})
    begin
      t = emit_documents(d)
      t = emit_documents(d)
      d.run
    rescue Mongo::OperationFailure => e
      assert_not_equal(11000, e.error_code)
    end

  end
end

class MongoReplOutputTest < MongoOutputTest
  def setup
    Fluent::Test.setup
    require 'fluent/plugin/out_mongo_replset'

    ensure_rs
  end

  def teardown
    @rs.restart_killed_nodes
    if defined?(@db) && @db
      @db.collection(collection_name).drop
      @db.connection.close
    end
  end

  def default_config
    %[
      type mongo_replset
      database #{MONGO_DB_DB}
      collection #{collection_name}
      nodes #{build_seeds(3).join(',')}
      num_retries 30
    ]
  end

  def create_driver(conf = default_config)
    @db = Mongo::MongoReplicaSetClient.new(build_seeds(3), :name => @rs.name).db(MONGO_DB_DB)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::MongoOutputReplset).configure(conf)
  end

  def test_configure
    d = create_driver(%[
      type mongo_replset

      database fluent_test
      collection test_collection
      nodes #{build_seeds(3).join(',')}
      num_retries 45

      capped
      capped_size 100
    ])

    assert_equal('fluent_test', d.instance.database)
    assert_equal('test_collection', d.instance.collection)
    assert_equal(build_seeds(3), d.instance.nodes)
    assert_equal(45, d.instance.num_retries)
    assert_equal({:capped => true, :size => 100}, d.instance.collection_options)
  end
end
