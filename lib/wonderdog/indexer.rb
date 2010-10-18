java_import 'org.elasticsearch.common.xcontent.XContentFactory'

#
# Add a new method to create a java indexable object for elasticsearch
#
Hash.class_eval do
  def to_indexable_json
    json_obj = XContentFactory.json_builder.start_object
    self.each do |k,v|
      json_obj.field(k.to_s.to_java_string, v.to_s.to_java_string)
    end
    json_obj.end_object
  end
end

module Wonderdog
  class Indexer < Client
    attr_writer :index, :type

    def initialize index, type
      @index = index.to_java_string
      @type  = type.to_java_string
      super
    end

    #
    # Insert a single json document into the index
    #
    def index_one_document doc_id, hsh
      doc_id   = doc_id.to_s.to_java_string
      response = @client.prepare_index(@index, @type, doc_id).set_source(hsh.to_indexable_json).execute.action_get
    end

  end
end
