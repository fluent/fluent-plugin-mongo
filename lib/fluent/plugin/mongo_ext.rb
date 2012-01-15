require 'mongo'

module Mongo
  class Collection
    # Temporary fix.
    # See pull request 82: https://github.com/mongodb/mongo-ruby-driver/pull/82
    def insert_documents(documents, collection_name=@name, check_keys=true, safe=false, flags={})
      if flags[:continue_on_error]
        message = BSON::ByteBuffer.new
        message.put_int(1)
      else
        message = BSON::ByteBuffer.new("\0\0\0\0")
      end

      collect_on_error = !!flags[:collect_on_error]
      error_docs = [] if collect_on_error

      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{collection_name}")
      documents =
        if collect_on_error
          documents.select do |doc|
            begin
              message.put_binary(BSON::BSON_CODER.serialize(doc, check_keys, true, @connection.max_bson_size).to_s)
              true
            rescue StandardError => e  # StandardError will be replaced with BSONError
              doc.delete(:_id)
              error_docs << doc
              false
            end
          end
        else
          documents.each do |doc|
            message.put_binary(BSON::BSON_CODER.serialize(doc, check_keys, true, @connection.max_bson_size).to_s)
          end
        end
      raise InvalidOperation, "Exceded maximum insert size of 16,000,000 bytes" if message.size > 16_000_000

      instrument(:insert, :database => @db.name, :collection => collection_name, :documents => documents) do
        if safe
          @connection.send_message_with_safe_check(Mongo::Constants::OP_INSERT, message, @db.name, nil, safe)
        else
          @connection.send_message(Mongo::Constants::OP_INSERT, message)
        end
      end

      doc_ids = documents.collect { |o| o[:_id] || o['_id'] }
      if collect_on_error
        return doc_ids, error_docs
      else
        doc_ids
      end
    end
  end
end

