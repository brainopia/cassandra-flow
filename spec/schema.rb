module Schema
  extend self
  ALL = {}

  def map(name, &block)
    ALL[name] = Cassandra::Mapper.new(:flow, name, &block)
  end

  def facts
    ALL[:facts]
  end

  def views
    ALL[:views]
  end
end

requests_schema = proc do
  key :project_id
  subkey :id
end

Schema.map :facts, &requests_schema
Schema.map :views, &requests_schema
