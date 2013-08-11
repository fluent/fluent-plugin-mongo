require 'spec_helper'
include Fluentd::PluginSpecHelper

require 'fluentd/plugin/in_mongo_tail'

describe Fluentd::Plugin::MongoTailInput do
  let(:config) {
    %[
      type mongo_tail
      database test
      collection log
      tag_key tag
      time_key time
      id_store_file /tmp/fluentd_mongo_last_id
    ]
  }

  it 'should accept correct configuration' do
    d = generate_driver(Fluentd::Plugin::MongoTailInput, config)
    expect(d.instance.host).to eql('localhost')
    expect(d.instance.port).to eql(27017)
    expect(d.instance.database).to eql('test')
    expect(d.instance.collection).to eql('log')
    expect(d.instance.tag_key).to eql('tag')
    expect(d.instance.time_key).to eql('time')
    expect(d.instance.id_store_file).to eql('/tmp/fluentd_mongo_last_id')
  end

  it 'should emit events' do
    # TODO: write actual code
    pending
  end
end
