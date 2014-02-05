require 'fluent/plugin/out_mongo'

module Fluent
  class MongoOutputTagCollection < MongoOutput
    Plugin.register_output('mongo_tag_collection', self)

    config_param :collection, :string, :default => 'untagged'

    def configure(conf)
      super

      @tag_mapped = true
      if remove_prefix_collection = conf['remove_prefix_collection']
        @remove_tag_prefix = Regexp.new('^' + Regexp.escape(remove_prefix_collection))
      end

      log.warn "'mongo_tag_collection' deprecated. Please use 'mongo' type with 'tag_mapped' parameter"
    end
  end
end
