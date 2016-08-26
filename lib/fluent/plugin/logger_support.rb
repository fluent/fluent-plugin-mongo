module Fluent
  module LoggerSupport
    def self.included(klass)
      klass.instance_eval {
        desc "MongoDB log level"
        config_param :mongo_log_level, :string, default: 'info'
      }
    end

    def configure_logger(mongo_log_level)
      Mongo::Logger.level = case @mongo_log_level.downcase
                            when 'fatal'
                              Logger::FATAL
                            when 'error'
                              Logger::ERROR
                            when 'warn'
                              Logger::WARN
                            when 'info'
                              Logger::INFO
                            when 'debug'
                              Logger::DEBUG
                            end
    end
  end
end
