class Cassandra::Flow::Action::MatchFirst < Cassandra::Flow::Action
  attr_reader :mapper, :action, :catalog, :flow

  def initialize(mapper, &action)
    @mapper = mapper
    @action = action
  end

  def setup!(flow)
    @flow    = flow
    @catalog = build_catalog

    mapper.config.dsl.after_insert do |match|
      key_data    = select(:key, match)
      subkey_data = select(:subkey, match)

      lock key_data do
        records = catalog.get key_data, start: subkey_data

        records.each do |record|
          catalog.remove record
          next_actions.propagate :remove, record[:action_result]

          record[:action_result] = action.call record[:action_data], match
          record.merge! subkey_data

          catalog.insert record
          next_actions.propagate :insert, record[:action_result]
        end
      end
    end
  end

  def propagate(type, data)
    key_data = select(:key, data)

    lock key_data do
      matched = mapper.one key_data
      result  = action.call data, matched

      if type == :insert
        catalog_record = key_data
        catalog_record.merge! matched ? select(:subkey, matched) : max_subkey
        catalog_record.merge! action_data: data, action_result: result
        catalog.insert catalog_record
      end

      result
    end
  end

  private

  def select(field, data)
    data.select {|k,_| mapper.config.send(field).include? k }
  end

  def max_subkey
    mapper.config.subkey.each_with_object({}) do |field, data|
      type        = mapper.config.types[field]
      data[field] = Cassandra::Mapper::Convert.max type
    end
  end

  def build_catalog
    keyspace = target.keyspace_base
    table    = target.table + 'match_first'
    config   = mapper.config

    Cassandra::Mapper.new keyspace, table do
      key *config.key
      subkey *config.subkey, :uuid
      type :action_data,   :yaml
      type :action_result, :yaml
      type :uuid, :uuid

      config.subkey.each do |field|
        type field, config.types[field]
      end

      before_insert do |data|
        data[:uuid] ||= Time.now
      end
    end
  end

  def target
    flow.actions.last.target
  end

  def lock(key, &block)
    super 'match_first:' + key.values.join('.'), &block
  end
end
