require 'json'
java_import 'org.elasticsearch.common.xcontent.XContentFactory'
java_import 'org.elasticsearch.action.index.IndexRequest'
java_import 'org.elasticsearch.client.action.bulk.BulkRequestBuilder'
java_import 'org.elasticsearch.action.ActionListener'
java_import 'org.elasticsearch.action.bulk.BulkItemResponse'
java_import 'org.elasticsearch.action.bulk.BulkResponse'
java_import 'org.elasticsearch.common.collect.Iterators'

#
# Creates an efficient binary structure that elastic search can index
#
Hash.class_eval do
  def indexable_json
    builder = XContentFactory.jsonBuilder().startObject()
    self.each do |k,v|
      builder.field(k.to_s.to_java_string, v.to_s.to_java_string)
    end
    builder.end_object
  end
end

module Wonderdog
  class Parser < Client
    attr_writer :index, :type

    def initialize index, type
      @index = index.to_java_string
      @type  = type.to_java_string
      super
    end

    #
    # Build and start a new indexer that does not do indexing itself. Instead
    # this client is responsible for parsing json input and creating binary data
    # to be indexed by an es_server. It is NOT allocated shards.
    #
    def client
      @client ||= es_node.client(true).data(false).node.client
    end

    #
    # Creates a bulk request object and sends it to an es_server running
    # somewhere else.
    #
    def index_array arr
      bulk = client.prepare_bulk
      arr.each do |doc|
        request = IndexRequest.new(@index).type(@type).create(true).source(doc.indexable_json)
        bulk.add(request)
      end
      warn("About to bulk index #{arr.size} documents, starting with #{arr.first.inspect}")
      bulk.execute.action_get
    end

    #
    # Allow for explicitly refreshing the index
    #
    def refresh_indices
      response = client.admin.indices.prepare_refresh([@index].to_java(:string)).execute.action_get
    end

    #
    # Insert a single json document into the index, don't use this except for debugging
    #
    def index_one_document hsh
      response = client.prepare_index(@index, @type).set_source(hsh.to_indexable_json).execute.action_get
    end

  end
end
