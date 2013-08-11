# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = "fluentd-plugin-mongo"
  gem.description = "MongoDB plugin for Fluentd event collector"
  gem.homepage    = "https://github.com/fluent/fluent-plugin-mongo"
  gem.summary     = gem.description
  gem.version     = File.read("VERSION").strip
  gem.authors     = ["Masahiro Nakagawa"]
  gem.email       = "repeatedly@gmail.com"
  gem.has_rdoc    = false
  #gem.platform    = Gem::Platform::RUBY
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_dependency "fluentd", "~> 0.11.0"
  gem.add_dependency "mongo", "~> 1.9.0"
  gem.add_development_dependency "rake", ">= 1.0"
  gem.add_development_dependency "rspec", "~> 2.13.0"
  gem.add_development_dependency "simplecov", ">= 0.5.4"
  gem.add_development_dependency "rr", ">= 1.0.0"
end
