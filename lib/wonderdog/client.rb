java_import 'org.elasticsearch.node.NodeBuilder'

module Wonderdog
  class Client

    def initialize *args
    end
    
    #
    # Instantiate a node builder but don't actually return a client
    #
    def es_node
      @es_node ||= NodeBuilder.node_builder
    end

    def client
      raise "Override this in your subclass!"
    end

  end
end
