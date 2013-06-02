class Cassandra::Flow::Action::UnlessMatch < Cassandra::Flow::Action::IfMatch
  def matches?(*)
    not super
  end
end
