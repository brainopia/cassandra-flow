class Cassandra::Flow::Action::Source < Cassandra::Flow::Action
  action! :source
  attr_reader :source

  def setup!(mapper)
    @source = mapper
    action  = self
    mapper.config.dsl do
      after_insert {|data| action.propagate_next :insert, data }
      after_remove {|data| action.propagate_next :remove, data }
    end
  end
end
