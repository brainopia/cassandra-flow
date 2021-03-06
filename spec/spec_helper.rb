require 'cassandra/flow'
require 'schema'

Cassandra::Mapper.schema = { keyspaces: [:flow] }
Cassandra::Mapper.env    = :test
Cassandra::Mapper.force_migrate

RSpec.configure do |config|
  config.include Schema

  config.before do
    Cassandra::Mapper.clear!
    Cassandra::Mapper.instances.each do |it|
      it.config.dsl.reset_callbacks!
    end
  end
end
