class Cassandra::Flow::Action::Label < Cassandra::Flow::Action
  action!

  def setup!(suffix)
    @suffix = suffix
  end
end
