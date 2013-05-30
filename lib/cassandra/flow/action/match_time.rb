# TODO: reinsert?
class Cassandra::Flow::Action::MatchTime < Cassandra::Flow::Action
  attr_reader :mapper, :action, :catalog, :flow,
    :source_time_field, :matched_time_field

  def initialize(mapper, source_time_field=nil, &action)
    @mapper = mapper
    @action = action
    @source_time_field = source_time_field
  end

  def setup!(flow)
    @flow    = flow
    @catalog = build_catalog

    @source_time_field ||= flow.source.config.subkey.first
    @matched_time_field = mapper.config.subkey.first

    mapper.config.dsl.after_insert do |match|
      key          = select(:key, match)
      matched_time = match[matched_time_field]

      lock key do
        # -0.001 instead of (slice: :before) because of uuid
        records = catalog.get key, start: { source_time: matched_time - 0.001 }
        records.select! do |it|
          not it[:matched_time] or
          it[:matched_time] <= match[matched_time_field]
        end

        records.each do |record|
          next_actions.propagate :remove, record[:action_result]
          record[:action_result] = action.call record[:action_data].dup, match
          next_actions.propagate :insert, record[:action_result]

          record[:matched_time] = matched_time
          catalog.insert record
        end
      end
    end

    mapper.config.dsl.after_remove do |match|
      key          = select(:key, match)
      matched_time = match[matched_time_field]

      lock key do
        records = catalog.get key, start: { source_time: matched_time }
        records.select! {|it| it[:matched_time] == match[matched_time_field] }

        records.each do |record|
          catalog.remove record
          next_actions.propagate :remove, record[:action_result]
          result = match_time :insert, key, record[:action_data]
          next_actions.propagate :insert, result
        end
      end
    end
  end

  def propagate(type, data)
    key = select(:key, data)

    lock key do
      match_time type, key, data
    end
  end

  private

  def match_time(type, key, data)
    source_time = data[source_time_field]

    unless source_time
      raise ArgumentError, "missing :#{source_time_field} in #{data.inspect}"
    end

    matched = mapper.one key,
                         reversed: true,
                         start: { matched_time_field => source_time, slice: :after }

    matched_time = matched[matched_time_field] if matched
    result       = action.call data.dup, matched
    subkey       = { source_time: source_time, matched_time: matched_time }

    if type == :insert
      catalog_record = key
      catalog_record.merge! subkey
      catalog_record.merge! action_data: data, action_result: result
      catalog.insert catalog_record
    elsif type == :remove
      catalog.remove key.merge(subkey)
    end

    result
  end

  def build_catalog
    keyspace = target.keyspace_base
    table    = target.table + '_match_time_' + mapper.table
    config   = mapper.config

    Cassandra::Mapper.new keyspace, table do
      key *config.key
      subkey :source_time
      type :action_data, :yaml
      type :action_result, :yaml
      type :source_time, :uuid
      type :matched_time, :time
    end
  end

  def select(field, data)
    data.select {|k,_| mapper.config.send(field).include? k }
  end

  def target
    flow.actions.last.target
  end

  def lock(key, &block)
    super 'match_time:' + key.values.join('.'), &block
  end
end
