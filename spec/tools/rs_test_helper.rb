$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'spec_helper'
require 'tools/repl_set_manager'

module ReplSetHelpers
  # Ensure replica set is available as an instance variable and that
  # a new set is spun up for each TestCase class
  def ensure_rs
    @rs = ReplSetManager.new
    @rs.start_set
  end

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure(max_retries=30)
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => ex
      puts "Rescue attempt #{retries}: from #{ex}"
      retries += 1
      raise ex if retries > max_retries
      sleep(2)
      retry
    end
  end
  
  def build_seeds(num_hosts)
    seeds = []
    num_hosts.times do |n|
      seeds << "#{@rs.host}:#{@rs.ports[n]}"
    end
    seeds
  end
end
