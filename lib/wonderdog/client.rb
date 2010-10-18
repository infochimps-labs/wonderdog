java_import 'org.elasticsearch.node.NodeBuilder'

module Wonderdog
  class Client
    
    def initialize
      @node   = NodeBuilder.node_builder.node
      @client = @node.client
    end

    def close
      @node.close
    end

    def cluster_name
      response = @client.admin.cluster.prepare_state.execute.action_get
      response.cluster_name.to_string
    end

  end
end
