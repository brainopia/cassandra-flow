# TODO: support removal
class Cassandra::Flow::Action::Flag < Cassandra::Flow::Action
  attr_reader :flow, :name, :scope, :condition, :catalog

  def initialize(name, scope, &condition)
    @name      = name
    @scope     = Array scope
    @condition = condition
  end

  def propagate(type, data)
    flag! data if type == :insert
    data
  end

  def setup!(flow)
    @flow    = flow
    @catalog = build_catalog
  end

  private

  def build_catalog
    keyspace = target.keyspace_base
    table    = target.table + '_flag'
    Cassandra::Mapper.new keyspace, table do
      key :scope
      type :data, :yaml
    end
  end

  def flag!(data)
    lock_name = 'flag:' + scope.map {|it| data[it] }.join('.')
    reflag, previous = []

    lock(lock_name) do
      record   = catalog.one scope: lock_name
      previous = record[:data] if record
      reflag   = !previous || condition.call(data, previous)

      if reflag
        catalog.insert scope: lock_name, data: data
        data[name] = true
      end
    end

    if reflag and previous
      next_actions.propagate :remove, previous.merge(name => true)
      next_actions.propagate :insert, previous
    end
  end

  def target
    flow.actions.last.target
  end
end
