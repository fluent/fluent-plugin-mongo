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

  def emit(tag, es, chain)
    super(tag, es, chain, tag)
  end

  def write(chunk)
    operate(chunk.key, collect_records(chunk))
  end
end


end
