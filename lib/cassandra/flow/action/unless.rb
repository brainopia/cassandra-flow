class Cassandra::Flow::Action::Unless < Cassandra::Flow::Action::If
  def matches?(*)
    not super
  end
end
