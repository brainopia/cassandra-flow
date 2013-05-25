Gem::Specification.new do |gem|
  gem.name          = 'cassandra-flow'
  gem.version       = '0.1'
  gem.authors       = 'brainopia'
  gem.email         = 'brainopia@evilmartians.com'
  gem.homepage      = 'https://github.com/brainopia/cassandra-flow'
  gem.summary       = 'Flexible workflow for cassandra'
  gem.description   = <<-DESCRIPTION
    First-class support for materialized views in cassandra.
  DESCRIPTION

  gem.files         = `git ls-files`.split($/)
  gem.test_files    = gem.files.grep %r{^spec/}
  gem.require_paths = %w(lib)

  gem.add_dependency 'cassandra-mapper'
  gem.add_development_dependency 'rspec'
end
