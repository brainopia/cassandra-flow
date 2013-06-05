class Cassandra::Flow::Action::Aggregate < Cassandra::Flow::Action
  action!
  attr_reader :scope, :callback, :catalog

  def setup!(scope, &callback)
    @scope    = Array scope
    @callback = callback

    append_name @scope.join('_')
    build_catalog
  end

  def propagate(type, data)
    lock_name = [name, scope.map {|it| data[it] }].join('.')

    lock lock_name do
      record   = catalog.one scope: lock_name
      all      = record ? record[:all] : []
      previous = record[:data].freeze if record

      case type
      when :insert
        all << data
        update = callback.call data, previous
      when :remove
        if all.index data
          all.delete_at all.index(data)
          update = all.inject(nil) {|previous, it| callback.call(it, previous) }
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
        propagate_next :remove, previous if previous
        propagate_next :insert, update if update
      end
    end
  end

  private

  def build_catalog
    @catalog = Cassandra::Mapper.new keyspace_name, name do
      key  :scope
      type :data, :yaml
      type :all,  :yaml
    end
  end
end
