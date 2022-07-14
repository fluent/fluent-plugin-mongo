= MongoDB plugin for {Fluentd}[http://github.com/fluent/fluentd]

fluent-plugin-mongo provides input and output plugins for {Fluentd}[http://fluentd.org] ({GitHub}[http://github.com/fluent/fluentd])

= Requirements

    |fluent-plugin-mongo|   fluentd  |  ruby  |
    |-------------------|------------|--------|
    |     >= 1.0.0      | >= 0.14.12 | >= 2.1 |
    |     <  1.0.0      | >= 0.12.0  | >= 1.9 |

= Installation

== Gems

The gem is hosted at {Rubygems.org}[http://rubygems.org]. You can install the gem as follows:

    $ fluent-gem install fluent-plugin-mongo

= Plugins

== Output plugin

=== mongo

Store Fluentd event to MongoDB database.

==== Configuration

Use _mongo_ type in match.

    <match mongo.**>
      @type mongo

      # You can choose two approaches, connection_string or each parameter
      # 1. connection_string for MongoDB URI
      connection_string mongodb://fluenter:10000/fluent

      # 2. specify each parameter
      database fluent
      host fluenter
      port 10000

      # collection name to insert
      collection test

      # Set 'user' and 'password' for authentication.
      # These options are not used when use connection_string parameter.
      user handa
      password shinobu

      # Set 'capped' if you want to use capped collection
      capped
      capped_size 100m

      # Specify date fields in record to use MongoDB's Date object (Optional) default: nil
      # Supported data types are String/Integer/Float/Fuentd EventTime.
      # For Integer type, milliseconds epoch and seconds epoch are supported.
      # eg: updated_at: "2020-02-01T08:22:23.780Z" or updated_at: 1580546457010
      date_keys updated_at

      # Specify id fields in record to use MongoDB's BSON ObjectID (Optional) default: nil
      # eg: my_id: "507f1f77bcf86cd799439011"
      object_id_keys my_id

      # Other buffer configurations here
    </match>

For _connection_string_ parameter, see https://docs.mongodb.com/manual/reference/connection-string/ article for more detail.

===== built-in placeholders

fluent-plugin-mongo support built-in placeholders.
_database_ and _collection_ parameters can handle them.

Here is an example to use built-in placeholders:

    <match mongo.**>
      @type mongo

      database ${tag[0]}

      # collection name to insert
      collection ${tag[1]}-%Y%m%d

      # Other buffer configurations here
      <buffer tag, time>
        @type memory
        timekey 3600
      </buffer>
    </match>

In more detail, please refer to the officilal document for built-in placeholders: https://docs.fluentd.org/v1.0/articles/buffer-section#placeholders

=== mongo(tag mapped mode)

Tag mapped to MongoDB collection automatically.

==== Configuration

Use _tag_mapped_ parameter in match of _mongo_ type.

If tag name is "foo.bar", auto create collection "foo.bar" and insert data.

    <match forward.*>
      @type mongo
      database fluent

      # Set 'tag_mapped' if you want to use tag mapped mode.
      tag_mapped

      # If tag is "forward.foo.bar", then prefix "forward." is removed.
      # Collection name to insert is "foo.bar".
      remove_tag_prefix forward.

      # This configuration is used if tag not found. Default is 'untagged'.
      collection misc

      # Other configurations here
    </match>

=== mongo_replset

Replica Set version of mongo.

==== Configuration

===== v0.8 or later

    <match mongo.**>
      @type mongo_replset
      database fluent
      collection logs

      nodes localhost:27017,localhost:27018

      # The replica set name
      replica_set myapp

      # num_retries is threshold at failover, default is 60.
      # If retry count reached this threshold, mongo plugin raises an exception.
      num_retries 30

      # following optional parameters passed to mongo-ruby-driver.
      # See mongo-ruby-driver docs for more detail: https://docs.mongodb.com/ruby-driver/master/tutorials/ruby-driver-create-client/
      # Specifies the read preference mode
      #read secondary
    </match>

===== v0.7 or ealier

Use _mongo_replset_ type in match.

    <match mongo.**>
      @type mongo_replset
      database fluent
      collection logs

      # each node separated by ','
      nodes localhost:27017,localhost:27018,localhost:27019

      # following optional parameters passed to mongo-ruby-driver.
      #name replset_name
      #read secondary
      #refresh_mode sync
      #refresh_interval 60
      #num_retries 60
    </match>

== Input plugin

=== mongo_tail

Tail capped collection to input data.

==== Configuration

Use _mongo_tail_ type in source.

    <source>
      @type mongo_tail
      database fluent
      collection capped_log

      tag app.mongo_log

      # waiting time when there is no next document. default is 1s.
      wait_time 5

      # Convert 'time'(BSON's time) to fluent time(Unix time).
      time_key time

      # Convert ObjectId to string
      object_id_keys ["id_key"]
    </source>

You can also use _url_ to specify the database to connect.

    <source>
      @type mongo_tail
      url mongodb://user:password@192.168.0.13:10249,192.168.0.14:10249/database
      collection capped_log
      ...
    </source>

This allows the plugin to read data from a replica set.

You can save last ObjectId to tail over server's shutdown to file.

    <source>
      ...

      id_store_file /Users/repeatedly/devel/fluent-plugin-mongo/last_id
    </source>

Or Mongo collection can be used to keep last ObjectID.

    <source>
      ...

      id_store_collection last_id
    </source>

Make sure the collection is capped. The plugin inserts records but does not remove at all.

= NOTE

== replace_dot_in_key_with and replace_dollar_in_key_with

BSON records which include '.' or start with '$' are invalid and they will be stored as broken data to MongoDB. If you want to sanitize keys, you can use _replace_dot_in_key_with_ and _replace_dollar_in_key_with_.

    <match forward.*>
      ...
      # replace '.' in keys with '__dot__'
      replace_dot_in_key_with __dot__

      # replace '$' in keys with '__dollar__'
      # Note: This replaces '$' only on first character
      replace_dollar_in_key_with __dollar__
      ...
    </match>

== Broken data as a BSON

NOTE: This feature will be removed since v0.8

Fluentd event sometimes has an invalid record as a BSON.
In such case, Mongo plugin marshals an invalid record using Marshal.dump
and re-inserts its to same collection as a binary.

If passed following invalid record:

    {"key1": "invalid value", "key2": "valid value", "time": ISODate("2012-01-15T21:09:53Z") }

then Mongo plugin converts this record to following format:

    {"__broken_data": BinData(0, Marshal.dump result of {"key1": "invalid value", "key2": "valid value"}), "time": ISODate("2012-01-15T21:09:53Z") }

Mongo-Ruby-Driver cannot detect an invalid attribute,
so Mongo plugin marshals all attributes excluding Fluentd keys("tag_key" and "time_key").

You can deserialize broken data using Mongo and Marshal.load. Sample code is below:

    # _collection_ is an instance of Mongo::Collection
    collection.find({'__broken_data' => {'$exists' => true}}).each do |doc|
      p Marshal.load(doc['__broken_data'].to_s) #=> {"key1": "invalid value", "key2": "valid value"}
    end

=== ignore_invalid_record

If you want to ignore an invalid record, set _true_ to _ignore_invalid_record_ parameter in match.

    <match forward.*>
      ...

      # ignore invalid documents at write operation
      ignore_invalid_record true

      ...
    </match>

=== exclude_broken_fields

If you want to exclude some fields from broken data marshaling, use _exclude_broken_fields_ to specfiy the keys.

    <match forward.*>
      ...

      # key2 is excluded from __broken_data.
      # e.g. {"__broken_data": BinData(0, Marshal.dump result of {"key1": "invalid value"}), "key2": "valid value", "time": ISODate("2012-01-15T21:09:53Z")
      exclude_broken_fields key2

      ...
    </match>

Specified value is a comma separated keys(e.g. key1,key2,key3).
This parameter is useful for excluding shard keys in shard environment.

== Buffer size limitation

Mongo plugin has the limitation of buffer size.
Because MongoDB and mongo-ruby-driver checks the total object size at each insertion.
If total object size gets over the size limitation, then
MongoDB returns error or mongo-ruby-driver raises an exception.

So, Mongo plugin resets _buffer_chunk_limit_ if configurated value is larger than above limitation:
- Before v1.8, max of _buffer_chunk_limit_ is 2MB
- After  v1.8, max of _buffer_chunk_limit_ is 8MB

= Tool

You can tail mongo capped collection.

    $ mongo-tail -f

= Test

Run following command:

    $ bundle exec rake test

You can use 'mongod' environment variable for specified mongod:

    $ mongod=/path/to/mongod bundle exec rake test

Note that source code in test/tools are from mongo-ruby-driver.

= Copyright

Copyright:: Copyright (c) 2011- Masahiro Nakagawa
License::   Apache License, Version 2.0
