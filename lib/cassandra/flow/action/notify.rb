class Cassandra::Flow::Action::Notify < Cassandra::Flow::Action::Check
  auto_setup!
  attr_reader :target
end
