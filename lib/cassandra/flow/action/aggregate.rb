class Cassandra::Flow::Action::Aggregate < Cassandra::Flow::Action
  attr_reader :flow, :scope, :callback, :catalog

  def initialize(scope, &callback)
    @scope    = Array scope
    @callback = callback
  end

  def propagate(type, data)
    lock_name = 'aggregate:' + scope.map {|it| data[it] }.join('.')

    lock lock_name do
      record   = catalog.one scope: lock_name
      all      = record ? record[:all] : []
      previous = record[:data] if record

      case type
      when :insert
        all << data
        update = callback.call data.dup, previous ? previous.dup : previous
      when :remove
        if all.index data
          all.delete_at all.index(data)
          update = all.inject(nil) {|previous, it| callback.call(it.dup, previous) }
        else
          update = previous
        end
      else
        raise ArgumentError, "unsupported type: #{type}"
      end

      if update
        catalog.insert scope: lock_name, data: update, all: all
      else
        catalog.remove scope: lock_name
      end

      if previous != update
        next_actions.propagate :remove, previous if previous
        next_actions.propagate :insert, update
      end
    end
    nil # stop propagating data, we handled it ourself
  end

  def setup!(flow)
    @flow    = flow
    @catalog = build_catalog
  end

  private

  def build_catalog
    table    = target.table + '_aggregate_' + scope.join('_')
    Cassandra::Mapper.new keyspace_name, table do
      key  :scope
      type :data, :yaml
      type :all,  :yaml
    end
  end

  def target
    flow.root.actions.last.target
  end
end
