class Cassandra::Flow::Action::Target < Cassandra::Flow::Action
  action!
  attr_reader :target

  def setup!(mapper)
    if mapper.is_a? Cassandra::Mapper
      @target = mapper
    else
      raise ArgumentError, 'target is nil'
    end
  end

  def transform(type, data)
    case type
    when :insert, :remove
      @target.send type, data
    when :check
      key = select :key, data
      objects = target.get key
      log_inspect key
      log_inspect objects
    end
    data
  end

  private

  def select(key_type, data)
    target.config.send(key_type).each_with_object({}) do |field, result|
      result[field] = data[field]
    end
  end
end
