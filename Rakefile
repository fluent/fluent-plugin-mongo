require 'bundler'
Bundler::GemHelper.install_tasks

require 'rspec/core'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |test|
  test.rspec_opts = %W[-c -f progress ]
  #test.libs << 'lib' << 'spec'
  test.pattern = 'spec/plugin/*_spec.rb'
  test.verbose = true
end

task :coverage do |t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task["spec"].invoke
end

task :default => [:build]
