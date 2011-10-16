require 'fluent/plugin/out_copy'
require 'fluent/plugin/out_mongo'


module Fluent


class MongoBackupOutput < CopyOutput
  Fluent::Plugin.register_output('mongo_backup', self)

  class MongoOutputForBackup < MongoOutput
    config_param :database, :string, :default => 'fluent'
    config_param :collection, :string, :default => 'out_mongo_backup'

    # TODO: optimize
  end

  def configure(conf)
    super

    backup = MongoOutputForBackup.new
    backup.configure(conf.merge({'capped' => true}))
    @outputs.unshift(backup)
  end
end


end
