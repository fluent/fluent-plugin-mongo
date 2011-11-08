module Fluent


class MongoTailInput < Input
  Plugin.register_input('mongo_tail', self)

  config_param :database, :string
  config_param :collection, :string
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 27017

  config_param :tag, :string, :default => nil
  config_param :tag_key, :string, :default => nil
  config_param :time_key, :string, :default => nil
  config_param :time_format, :string, :default => nil

  def initialize
    require 'mongo'
    super
  end

  def configure(conf)
    super

    if !@tag && !@tag_key
      raise ConfigError, "'tag' or 'tag_key' option is required on mongo_tail input"
    end
  end

  def start
    super
    @client = get_capped_collection
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @thread.join
    @client.db.connection.close
    super
  end

  def run
    last_id = nil
    loop {
      last_id = tailoop(Mongo::Cursor.new(@client, cursor_conf(last_id)))
    }
  end

  private

  def get_capped_collection
    db = Mongo::Connection.new(@host, @port).db(@database)
    raise ConfigError, "'#{@database}.#{@collection}' not found: server = #{@host}:#{@port}" unless db.collection_names.include?(@collection)
    collection = db.collection(@collection)
    raise ConfigError, "'#{@database}.#{@collection}' is not capped: server = #{@host}:#{@port}" unless collection.capped?
    collection
  end

  def tailoop(cursor)
    last_id = nil
    loop {
      cursor = Mongo::Cursor.new(@client, cursor_conf(last_id)) unless cursor.alive?
      if doc = cursor.next_document
        time = if @time_key
                 t = doc.delete(@time_key)
                 t.nil? ? Time.now : t.to_i
               else
                 Time.now
               end
        tag = if @tag_key
                t = doc.delete(@tag_key)
                t.nil? ? 'mongo.missing_tag' : t
              else
                @tag
              end
        # TODO: Stored to persistent system
        last_id = doc['_id']
        Engine.emit(tag, time, doc)
      end
    }
  rescue
    # ignore Mongo::OperationFailuer at CURSOR_NOT_FOUND
    last_id
  end

  def cursor_conf(last_id)
    cursor_conf = {}
    cursor_conf[:tailable] = true
    cursor_conf[:selector] = {'_id' => {'$gt' => last_id}} if last_id
    cursor_conf
  end
end


end
