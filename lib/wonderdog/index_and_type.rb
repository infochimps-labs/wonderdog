module Wukong
  module Elasticsearch

    # A convenient class for parsing Elasticsearch index and type URIs
    # like
    #
    #   - es://my_index
    #   - es://my_index/my_type
    #   - es://first_index,second_index,third_index
    #   - es://my_index/first_type,second_type,third_type
    class IndexAndType

      # The Elasticsearch index.
      #
      # @param [String]
      attr_reader :index

      # The Elasticsearch type.
      #
      # @param [String]
      attr_reader :type

      # Create a new index and type specification from the given
      # +uri..
      #
      # @param [String] uri
      def initialize uri
        self.uri = uri
      end

      # Set the URI of this index and type specification, parsing it
      # for an index and type.
      #
      # Will raise an error if the given URI is malformed.
      #
      # @param [String] uri
      def self.uri= uri
        begin
          @uri   = URI.parse(uri)
          path   = File.join(@uri.host, @uri.path)
          @index = path.split('/')[0]
          @type  = path.split('/')[1]
        rescue URI::InvalidURIError => e
          raise Wukong::Error.new("Could not parse '#{uri}' as an ElasticSearch /index/type specification")
        end
      end
    end
  end
end

    
    
