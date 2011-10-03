require 'fluent/plugin/out_copy'
require 'fluent/plugin/out_mongo'


module Fluent


class MongoBackupOutput < CopyOutput
  Fluent::Plugin.register_output('mongo_backup', self)

  class MongoOutputForBackup < MongoOutput
    def initialize
      super

      # default parameters
      @database_name = 'fluent'
      @collection_name = 'out_mongo_backup'
    end
  end

  def configure(conf)
    super

    backup = MongoOutputForBackup.new
    backup.configure(conf.merge({'capped' => true}))
    @outputs.unshift(backup)
  end
end


end
