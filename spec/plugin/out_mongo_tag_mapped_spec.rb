require 'spec_helper'
include Fluentd::PluginSpecHelper

require 'fluentd/plugin/out_mongo'

describe 'Fluentd::Plugin::MongoOutput with tag_mapped' do
  let(:default_config) {
    %[
      type mongo
      database fluentd
      tag_mapped
      remove_tag_prefix should.remove.
    ]
  }

  def create_driver(conf = default_config)
    generate_driver(Fluentd::Plugin::MongoOutput, conf)
  end

  it 'test_configure' do
    d = create_driver
    expect(d.instance.instance_variable_get(:@remove_tag_prefix)).to eql(/^should\.remove\./)
  end

  it 'test_write' do
    pending 'Refactor with emit arguments(chain and key)'

    d = create_driver
    d.run { |d|
      time = Time.parse("2011-01-02 13:14:15 UTC").to_i
      d.with('mytag', time) { |d|
        d.pitch({'a' => 1})
        d.pitch({'a' => 2})
      }
    }
    mock(d.instance).operate('mytag', [{'a' => 1, d.instance.time_key => Time.at(t)},
                                       {'a' => 2, d.instance.time_key => Time.at(t)}])
  end

  it 'test_remove_prefix_collection' do
    d = create_driver
    expect(d.instance.__send__(:format_collection_name, 'should.remove.prefix')).to eql('prefix')
    expect(d.instance.__send__(:format_collection_name, '..test..')).to eql('test')
    expect(d.instance.__send__(:format_collection_name, '..test.foo.')).to eql('test.foo')
  end
end
