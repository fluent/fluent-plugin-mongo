# -*- coding: utf-8 -*-
require 'spec_helper'
include Fluentd::PluginSpecHelper

require 'fluentd/plugin/out_mongo'

shared_examples_for 'basic_specs' do
  let(:tag) { 
    'mongo'
  }

  it 'test_configure_with_write_concern' do
    d = create_driver(default_config + %[
      write_concern 2
    ])

    expect(d.instance.connection_options).to eql({:w => 2})
  end

  it 'test_format' do
    d = create_driver
    d.run { |d|
      t = Time.parse("2011-01-02 13:14:15 UTC").to_i
      d.with(tag, t) { |d|
        d.pitch({'a' => 1})
        d.pitch({'a' => 2})
      }
    }
    #d.expect_format([time, {'a' => 1, d.instance.time_key => time}].to_msgpack)
    #d.expect_format([time, {'a' => 2, d.instance.time_key => time}].to_msgpack)

    expect(db.collection(collection_name).count).to eql(2)
  end

  def emit_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.with(tag, time) { |d|
      d.pitch({'a' => 1})
      d.pitch({'a' => 2})
    }
    time
  end

  def get_documents
    db.collection(collection_name).find().to_a.map { |e| e.delete('_id'); e }
  end

  it 'test_write' do
    create_driver.run { |d|
      emit_documents(d)
    }

    documents = get_documents.map { |e| e['a'] }.sort
    expect(documents).to eql([1, 2])
    expect(documents.size).to eql(2)
  end

=begin
  # This moved to record_modifier
  it 'test_write_at_enable_tag' do
    d = create_driver(default_config + %[
      include_tag_key true
      include_time_key false
    ])
    d.run { |d|
      emit_documents(d)
    }

    documents = get_documents.sort_by { |e| e['a'] }
    expect(documents).to eql([{'a' => 1, d.instance.tag_key => 'test'}, {'a' => 2, d.instance.tag_key => 'test'}])
    expect(documents.size).to eql(2)
  end
=end

  def emit_invalid_documents(d)
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.with(tag, time) { |d|
      d.pitch({'a' => 3, 'b' => "c", '$last' => '石動'})
      d.pitch({'a' => 4, 'b' => "d", 'first' => '菖蒲'.encode('EUC-JP').force_encoding('UTF-8')})
    }
    time
  end

  it 'test_write_with_invalid_recoreds' do
    d = create_driver
    d.run { |d|
      emit_documents(d)
      emit_invalid_documents(d)
    }

    documents = get_documents
    expect(documents.size).to eql(4)
    expect(documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort).to eql([1, 2])
    expect(documents.select { |e| e.has_key?(Fluentd::Plugin::MongoOutput::BROKEN_DATA_KEY)}.size).to eql(2)
    expect(db.collection(collection_name).find({Fluentd::Plugin::MongoOutput::BROKEN_DATA_KEY => {'$exists' => true}}).map { |doc|
        Marshal.load(doc[Fluentd::Plugin::MongoOutput::BROKEN_DATA_KEY].to_s)['a'] }.sort).to eql([3, 4])
  end

  it 'test_write_with_invalid_recoreds_with_exclude_one_broken_fields' do
    d = create_driver(default_config + %[
      exclude_broken_fields [a]
    ])
    d.run { |d|
      emit_documents(d)
      emit_invalid_documents(d)
    }

    documents = get_documents
    expect(documents.size).to eql(4)
    expect(documents.select { |e| e.has_key?(Fluentd::Plugin::MongoOutput::BROKEN_DATA_KEY) }.size).to eql(2)
    expect(documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort).to eql([1, 2, 3, 4])
    expect(documents.select { |e| e.has_key?('b') }.size).to eql(0)
  end

  it 'test_write_with_invalid_recoreds_with_exclude_two_broken_fields' do
    d = create_driver(default_config + %[
      exclude_broken_fields [a, b]
    ])
    d.run { |d|
      emit_documents(d)
      emit_invalid_documents(d)
    }

    documents = get_documents
    expect(documents.size).to eql(4)
    expect(documents.select { |e| e.has_key?(Fluentd::Plugin::MongoOutput::BROKEN_DATA_KEY) }.size).to eql(2)
    expect(documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort).to eql([1, 2, 3, 4])
    expect(documents.select { |e| e.has_key?('b') }.map { |e| e['b'] }.sort).to eql(["c", "d"])
  end

  it 'test_write_with_invalid_recoreds_at_ignore' do
    d = create_driver(default_config + %[
      ignore_invalid_record true
    ])
    d.run { |d|
      emit_documents(d)
      emit_invalid_documents(d)
    }

    documents = get_documents
    expect(documents.size).to eql(2)
    expect(documents.select { |e| e.has_key?('a') }.map { |e| e['a'] }.sort).to eql([1, 2])
    expect(db.collection(collection_name).find(Fluentd::Plugin::MongoOutput::BROKEN_DATA_KEY => {'$exists' => true}).count.zero?).to be_true
  end
end

describe Fluentd::Plugin::MongoOutput, :mongod => true do
  it_should_behave_like 'basic_specs'

  let(:collection_name) {
    'single_test'
  }

  before(:all) { 
    setup_mongod
  }

  after(:each) { 
    db.collection(collection_name).drop
    teardown_mongod
  }

  after(:all) { 
    cleanup_mongod_env
  }

  let(:default_config) {
    %[
      type mongo
      database #{MONGO_DB_DB}
      collection #{collection_name}
    ]
  }

  let(:db) { 
    Mongo::MongoClient.new('localhost', mongod_port).db(MONGO_DB_DB)
  }

  def create_driver(conf = default_config)
    conf = conf + %[
      port #{mongod_port}
    ]
    #Fluentd::Test::BufferedOutputTestDriver.new(Fluentd::MongoOutput).configure(conf)
    generate_driver(Fluentd::Plugin::MongoOutput, conf)
  end

  it 'test_configure' do
    d = create_driver(%[
      type mongo
      database fluentd_test
      collection test_collection

      capped
      capped_size 100
    ])

    expect(d.instance.database).to eql('fluentd_test')
    expect(d.instance.collection).to eql('test_collection')
    expect(d.instance.host).to eql('localhost')
    expect(d.instance.port).to eql(mongod_port)
    expect(d.instance.collection_options).to eql(:capped => true, :size => 100)
    expect(d.instance.connection_options.empty?).to be_true
    # buffer_chunk_limit moved from configure to start
    # I will move this test to correct space after BufferedOutputTestDriver supports start method invoking
    # expect(Fluentd::MongoOutput::LIMIT_BEFORE_v1_8, d.instance.instance_variable_get(:@buffer).buffer_chunk_limit)
  end
end

require 'fluentd/plugin/out_mongo_replset'

describe Fluentd::Plugin::MongoOutputReplset, :replset => true do
  # In v11, test_format and test_write_with_invalid_recoreds_with_exclude_one_broken_fields are failed but
  # I don't know why these specs are failed. Maybe timing issue.
  it_should_behave_like 'basic_specs'

  let(:collection_name) {
    'replica_test'
  }

  before(:all) { 
    ensure_rs
  }

  after(:each) { 
    db.collection(collection_name).drop
    @rs.restart_killed_nodes
    db.connection.close
  }

  let(:default_config) {
    %[
      type mongo_replset
      database #{MONGO_DB_DB}
      collection #{collection_name}
      nodes ["#{build_seeds(3).join('","')}"]
      num_retries 30
    ]
  }

  let(:db) { 
    Mongo::MongoReplicaSetClient.new(build_seeds(3), :name => @rs.name).db(MONGO_DB_DB)
  }

  def create_driver(conf = default_config)
    generate_driver(Fluentd::Plugin::MongoOutputReplset, conf)
  end

  it 'test_configure' do
    d = create_driver(%[
      type mongo_replset

      database fluentd_test
      collection test_collection
      nodes ["#{build_seeds(3).join('","')}"]
      num_retries 45

      capped
      capped_size 100
    ])

    expect(d.instance.database).to eql('fluentd_test')
    expect(d.instance.collection).to eql('test_collection')
    expect(d.instance.nodes).to eql(build_seeds(3))
    expect(d.instance.num_retries).to eql(45)
    expect(d.instance.collection_options).to eql(:capped => true, :size => 100)
  end
end
