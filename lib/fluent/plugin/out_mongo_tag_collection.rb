
require 'fluent/plugin/out_mongo'

module Fluent

class MongoOutputTagCollection < MongoOutput
  Fluent::Plugin.register_output('mongo_tag_collection', self)

  def configure(conf)
    super
    if remove_prefix_collection = conf['remove_prefix_collection']
      @remove_prefix_collection = Regexp.new('^' + Regexp.escape(remove_prefix_collection))
    end
  end

  def format(tag, time, record)
    [tag, record].to_msgpack
  end

  def write(chunk)
    collections = {}

    chunk.msgpack_each { |tag, record|
      record[@time_key] = Time.at(record[@time_key]) if @include_time_key
      (collections[tag] ||= []) << record
    }

    collections.each { |collection_name, records|
      operate(collection_name, records)
    }
  end
end

end
