module Schema
  extend self
  ALL = {}

  def map(name, &block)
    ALL[name] = Cassandra::Mapper.new(:flow, name, &block)
  end

  def facts
    ALL[:facts]
  end

  def facts2
    ALL[:facts2]
  end

  def views
    ALL[:views]
  end
end

basic_schema = proc do
  key :project_id
  subkey :id

  type :project_id, :integer
  type :id,         :integer
  type :matched_id, :integer
  type :archive,    :boolean
  type :min,        :boolean
end

Schema.map :facts, &basic_schema
Schema.map :facts2, &basic_schema
Schema.map :views, &basic_schema
