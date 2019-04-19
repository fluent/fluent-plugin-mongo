module Fluent
  module MongoAuthParams
    def self.included(klass)
      klass.instance_eval {
        desc "MongoDB user"
        config_param :user, :string, default: nil
        desc "MongoDB password"
        config_param :password, :string, default: nil, secret: true
        desc "MongoDB authentication database"
        config_param :auth_source, :string, default: nil
        desc "MongoDB authentication mechanism"
        config_param :auth_mech, :string, default: nil
      }
    end
  end

  module MongoAuth
    def authenticate(client)
      begin
        if [@user, @password, @auth_source].all?
          client = client.with(user: @user, password: @password, auth_source: @auth_source)
        elsif [@user, @password].all?
          client = client.with(user: @user, password: @password)
        elsif [@user, @auth_source, @auth_mech].all?
          client = client.with(user: @user, auth_source: @auth_source, auth_mech: @auth_mech.to_sym)
        end
      rescue Mongo::Auth::Unauthorized => e
        log.fatal e
        exit!
      end
      client
    end
  end
end
