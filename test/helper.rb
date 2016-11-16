require 'test/unit'
require 'fluent/test'
require 'mongo'
require 'fluent/plugin/out_mongo'
require 'fluent/plugin/out_mongo_replset'
require 'fluent/plugin/in_mongo_tail'
require 'fluent/mixin' # for TimeFormatter

def time_formatter(time, time_format: nil, localtime: true, timezone: true)
  formatter = Fluent::TimeFormatter.new(time_format, localtime, timezone)
  formatted_time = formatter.call(time)
  formatted_time
end
