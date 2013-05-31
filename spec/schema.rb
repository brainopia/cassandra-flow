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

  def events
    ALL[:events]
  end

  def events2
    ALL[:events2]
  end

  def event_map
    ALL[:event_map]
  end

  def diff_event_map
    ALL[:diff_event_map]
  end
end

basic_schema = proc do
  key :project_id
  subkey :id

  type :project_id, :integer
  type :id,         :integer
  type :matched_id, :integer
  type :total,      :integer
  type :archive,    :boolean
  type :min,        :boolean
end

Schema.map :facts, &basic_schema
Schema.map :facts2, &basic_schema
Schema.map :views, &basic_schema

def timeline_schema(field=:time)
  proc do
    key :project_id
    subkey field
    type field, :time
    type :id, :integer
    type :project_id, :integer
    type :matched_id, :integer
  end
end

Schema.map :events, &timeline_schema
Schema.map :events2, &timeline_schema
Schema.map :event_map, &timeline_schema
Schema.map :diff_event_map, &timeline_schema(:diff)
