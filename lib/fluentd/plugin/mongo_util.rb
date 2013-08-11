module Fluentd
  module MongoUtil
    def self.included(klass)
      klass.instance_eval {
        config_param :user, :string, :default => nil
        config_param :password, :string, :default => nil
      }
    end

    def authenticate(db)
      unless @user.nil? || @password.nil?
        begin
          db.authenticate(@user, @password)
        rescue Mongo::AuthenticationError => e
          $log.fatal e
          exit!
        end
      end

      db
    end
  end
end
