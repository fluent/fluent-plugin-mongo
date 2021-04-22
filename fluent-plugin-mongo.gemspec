# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = "fluent-plugin-mongo"
  gem.description = "MongoDB plugin for Fluentd"
  gem.homepage    = "https://github.com/fluent/fluent-plugin-mongo"
  gem.summary     = gem.description
  gem.licenses    = ["Apache-2.0"]
  gem.version     = File.read("VERSION").strip
  gem.authors     = ["Masahiro Nakagawa"]
  gem.email       = "repeatedly@gmail.com"
  #gem.platform    = Gem::Platform::RUBY
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_dependency "fluentd", [">= 0.14.22", "< 2"]
  gem.add_runtime_dependency "mongo", "~> 2.13.0"
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "simplecov", ">= 0.5.4"
  gem.add_development_dependency "rr", ">= 1.0.0"
  gem.add_development_dependency "test-unit", ">= 3.0.0"
  gem.add_development_dependency "timecop", "~> 0.8.0"
  gem.add_development_dependency "webrick", ">= 1.7.0"
end
