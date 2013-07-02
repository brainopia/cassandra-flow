class Cassandra::Flow::Action::Flag < Cassandra::Flow::Action
  action!
  attr_reader :flag, :scope, :condition, :catalog
  LIMIT_HISTORY = 5

  def setup!(name, scope, &condition)
    @flag      = name
    @scope     = Array scope
    @condition = condition

    append_name @flag.to_s
    append_name @scope.join('_')
    build_catalog
  end

  def transform(type, data)
    lock_name = [name, scope.map {|it| data[it] }].join('.')

    lock lock_name do
      record   = catalog.one scope: lock_name
      previous = record[:data] if record
      all      = record ? record[:all] : []

      case type
      when :insert
        all << data
        reflag = !previous || condition.call(data, previous)

        if all.size >= 2*LIMIT_HISTORY
          all = all.sort {|a,b| condition.call(a,b) ? -1 : 1 }.first(LIMIT_HISTORY)
        end

        if reflag
          catalog.insert scope: lock_name, data: data, all: all
          data = data.dup
          data[flag] = true
        else
          catalog.insert scope: lock_name, data: previous, all: all
        end

        if reflag and previous
          propagate_next :remove, previous.merge(flag => true)
          propagate_next :insert, previous
        end
      when :remove
        if all.delete data
          if data == previous
            data = data.dup
            data[flag] = true

            new_data = all.sort {|a,b| condition.call(a,b) ? -1 : 1 }.first
            if new_data
              propagate_next :remove, new_data
              catalog.insert scope: lock_name, data: new_data, all: all
              propagate_next :insert, new_data.merge(flag => true)
            else
              catalog.remove scope: lock_name
            end
          else
            catalog.insert scope: lock_name, data: previous, all: all
          end
        end
      when :check
        log_inspect lock_name
        log_inspect all

        if data == all.sort {|a,b| condition.call(a,b) ? -1 : 1 }.first
          data = data.merge flag => true
        end
      else
        raise ArgumentError, "unsupported type: #{type}"
      end

      data
    end
  end

  private

  def build_catalog
    @catalog = Cassandra::Mapper.new keyspace_name, name do
      key  :scope
      type :data, :marshal
      type :all,  :marshal
    end
  end
end
