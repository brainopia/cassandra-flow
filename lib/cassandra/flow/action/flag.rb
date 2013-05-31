class Cassandra::Flow::Action::Flag < Cassandra::Flow::Action
  attr_reader :flow, :name, :scope, :condition, :catalog

  def initialize(name, scope, &condition)
    @name      = name
    @scope     = Array scope
    @condition = condition
  end

  def propagate(type, data)
    lock_name = 'flag:' + scope.map {|it| data[it] }.join('.')

    lock(lock_name) do
      record   = catalog.one scope: lock_name
      previous = record[:data] if record
      all      = record ? record[:all] : []

      case type
      when :insert
        all << data
        reflag = !previous || condition.call(data, previous)

        if reflag
          catalog.insert scope: lock_name, data: data, all: all
          data[name] = true
        else
          catalog.insert scope: lock_name, data: previous, all: all
        end

        if reflag and previous
          next_actions.propagate :remove, previous.merge(name => true)
          next_actions.propagate :insert, previous
        end
      when :remove
        if all.delete data
          if data == previous
            data[name] = true
            new_data = all.sort {|a,b| condition.call(a,b) ? -1 : 1 }.first
            if new_data
              next_actions.propagate :remove, new_data
              catalog.insert scope: lock_name, data: new_data, all: all

              new_data[name] = true
              next_actions.propagate :insert, new_data
            else
              catalog.remove scope: lock_name
            end
          else
            catalog.insert scope: lock_name, data: previous, all: all
          end
        end
      else
        raise ArgumentError, "unsupported type: #{type}"
      end

      data
    end
  end

  def setup!(flow)
    @flow    = flow
    @catalog = build_catalog
  end

  private

  def build_catalog
    keyspace = target.keyspace_base
    table    = target.table + '_flag_' + name.to_s
    Cassandra::Mapper.new keyspace, table do
      key  :scope
      type :data, :yaml
      type :all,  :yaml
    end
  end

  def target
    flow.actions.last.target
  end
end
