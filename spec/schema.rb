Mapper = {}

def mapper(name, &block)
  Mapper[name] = Cassandra::Mapper.new(:flow, name, &block)
end

def reset(*mappers)
  mappers.each do |mapper|
    mapper.keyspace.drop_column_family mapper.table rescue nil
    mapper.migrate
    mapper.config.dsl.reset_callbacks!
  end
end

requests_schema = proc do
  key :project_id
  subkey :id
end

mapper :requests, &requests_schema
mapper :requests_backup, &requests_schema
