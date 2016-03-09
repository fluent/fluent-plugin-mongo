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
      }
    end
  end

  module MongoAuth
    def authenticate(client)
      unless @user.nil? || @password.nil?
        begin
          if @auth_source.nil?
            client = client.with(user: @user, password: @password)
          else
            client = client.with(user: @user, password: @password, auth_source: @auth_source)
          end
        rescue Mongo::Auth::Unauthorized => e
          log.fatal e
          exit!
        end
      end
      client
    end
  end
end
