require 'cassandra/flow'
require 'schema'

Cassandra::Mapper.schema = { keyspaces: [ :flow ]}
Cassandra::Mapper.env    = :test

begin
  Cassandra::Mapper.migrate
rescue CassandraThrift::InvalidRequestException
  puts 'Using existing keyspace'
end
