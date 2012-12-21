module Wukong
  module Elasticsearch

    # A convenient class for parsing Elasticsearch index and mapping URIs
    # like
    #
    #   - es://my_index
    #   - es://my_index/my_mapping
    #   - es://first_index,second_index,third_index
    #   - es://my_index/first_mapping,second_mapping,third_mapping
    class IndexAndMapping

      # A regular expression that matches URIs describing an
      # Elasticsearch index and/or mapping to read/write from/to.
      #
      # @param [Regexp]
      ES_SCHEME_REGEXP        = %r{^es://}
      
      # The Elasticsearch index.
      #
      # @param [String]
      attr_reader :index

      # The Elasticsearch mapping.
      #
      # @param [String]
      attr_reader :mapping

      # Does the given `string` look like a possible Elasticsearch
      # /index/mapping specification?
      #
      # @param [String] string
      # @return [true, false]
      def self.matches? string
        return false unless string
        string =~ ES_SCHEME_REGEXP
      end

      # Create a new index and mapping specification from the given
      # +uri..
      #
      # @param [String] uri
      def initialize uri
        self.uri = uri
      end

      # Set the URI of this index and mapping specification, parsing it
      # for an index and mapping.
      #
      # Will raise an error if the given URI is malformed.
      #
      # @param [String] uri
      def uri= uri
        raise Wukong::Error.new("'#{uri}' is not an ElasticSearch es://index/mapping specification") unless self.class.matches?(uri)
        parts = uri.gsub(ES_SCHEME_REGEXP, '').gsub(/^\/+/,'').gsub(/\/+$/,'').split('/')
        
        raise Wukong::Error.new("'#{uri}' is not an ElasticSearch es://index/mapping specification") unless parts.size.between?(1,2)

        @index   = parts[0]
        @mapping = parts[1]
      end
    end
  end
end

    
    
