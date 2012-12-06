require_relative("index_and_mapping")

module Wukong
  module Elasticsearch

    # This module overrides some methods defined in
    # Wukong::Hadoop::HadoopInvocation.  The overrides will only come
    # into play if the job's input or output paths are URIs beginning
    # with 'es://', implying reading or writing to/from Elasticsearch
    # indices.
    module HadoopInvocationOverride

      # The input format when reading from Elasticsearch as defined in
      # the Java code accompanying Wonderdog.
      #
      # @param [String]
      ES_STREAMING_INPUT_FORMAT  = "com.infochimps.elasticsearch.ElasticSearchStreamingInputFormat"

      # The output format when writing to Elasticsearch as defined in
      # the Java code accompanying Wonderdog.
      #
      # @param [String]
      ES_STREAMING_OUTPUT_FORMAT = "com.infochimps.elasticsearch.ElasticSearchStreamingOutputFormat"

      # Does this job read from Elasticsearch?
      #
      # @return [true, false]
      def reads_from_elasticsearch?
        IndexAndMapping.matches?(settings[:input])
      end

      # The input format to use for this job.
      #
      # Will override the default value to ES_STREAMING_INPUT_FORMAT if
      # reading from Elasticsearch.
      #
      # @return [String]
      def input_format
        reads_from_elasticsearch? ? ES_STREAMING_INPUT_FORMAT : super()
      end

      # The input index to use.
      #
      # @return [IndexAndMapping]
      def input_index
        @input_index ||= IndexAndMapping.new(settings[:input])
      end

      # The input paths to use for this job.
      #
      # Will override the default value with a temporary HDFS path
      # when reading from Elasticsearch.
      #
      # @return [String]
      def input_paths
        reads_from_elasticsearch? ? elasticsearch_hdfs_tmp_dir(input_index) : super()
      end

      # Does this write to Elasticsearch?
      #
      # @return [true, false]
      def writes_to_elasticsearch?
        IndexAndMapping.matches?(settings[:output])
      end

      # The output format to use for this job.
      #
      # Will override the default value to ES_STREAMING_OUTPUT_FORMAT if
      # writing to Elasticsearch.
      #
      # @return [String]
      def output_format
        writes_to_elasticsearch? ? ES_STREAMING_OUTPUT_FORMAT : super()
      end

      # The output index to use.
      #
      # @return [IndexAndMapping]
      def output_index
        @output_index ||= IndexAndMapping.new(settings[:output])
      end

      # The output path to use for this job.
      #
      # Will override the default value with a temporary HDFS path
      # when writing to Elasticsearch.
      #
      # @return [String]
      def output_path
        writes_to_elasticsearch? ? elasticsearch_hdfs_tmp_dir(output_index) : super()
      end

      # Adds Java options required to interact with the input/output
      # formats defined by the Java code accompanying Wonderdog.
      #
      # Will not change the default Hadoop jobconf options unless it
      # has to.
      #
      # @return [Array<String>]
      def hadoop_jobconf_options
        super() + [].tap do |o|
          o << java_opt('es.config', settings[:es_config]) if (reads_from_elasticsearch? || writes_to_elasticsearch?)
          
          if reads_from_elasticsearch?
            o << java_opt('elasticsearch.input.index',          input_index.index)
            o << java_opt('elasticsearch.input.mapping',        input_index.mapping)
            o << java_opt('elasticsearch.input.splits',         settings[:es_input_splits])
            o << java_opt('elasticsearch.input.query',          settings[:es_query])
            o << java_opt('elasticsearch.input.request_size',   settings[:es_request_size])
            o << java_opt('elasticsearch.input.scroll_timeout', settings[:es_scroll_timeout])
          end

          if writes_to_elasticsearch?
            o << java_opt('elasticsearch.output.index',         output_index.index)
            o << java_opt('elasticsearch.output.mapping',       output_index.mapping)
            o << java_opt('elasticsearch.output.index.field',   settings[:es_index_field])
            o << java_opt('elasticsearch.output.mapping.field', settings[:es_mapping_field])
            o << java_opt('elasticsearch.output.id.field',      settings[:es_id_field])
            o << java_opt('elasticsearch.output.bulk_size',     settings[:es_bulk_size])
          end
        end.flatten.compact
      end

      # :nodoc:
      #
      # Munge the settings object to add necessary jars if
      # reading/writing to/from Elasticsearch, then call super().
      def hadoop_files
        if reads_from_elasticsearch? || writes_to_elasticsearch?
          settings[:jars] = elasticsearch_jars if settings[:jars].empty?
        end
        super()
      end

      # All Elasticsearch, Wonderdog, and other support jars needed to
      # connect Hadoop streaming with the
      # ElasticSearchStreamingInputFormat and
      # ElasticSearchStreamingOutputFormat provided by the Wonderdog
      # Java code.
      #
      # @return [Array<String>]
      def elasticsearch_jars
        Dir[File.join(settings[:es_lib_dir] || '/usr/lib/hadoop/lib', '{elasticsearch,lucene,jna,wonderdog}*.jar')].compact.uniq
      end

      # Returns a temporary path on the HDFS in which to store log
      # data while the Hadoop job runs.
      #
      # @param [IndexAndMapping] io
      # @return [String]
      def elasticsearch_hdfs_tmp_dir io
        cleaner  = %r{[^\w/\.\-\+]+}
        io_part  = [io.index, io.mapping].compact.map { |s| s.gsub(cleaner, '') }.join('/')
        File.join(settings[:es_tmp_dir] || '/', io_part || '', Time.now.strftime("%Y-%m-%d-%H-%M-%S"))
      end

    end
  end

  Hadoop::Driver.class_eval { include Elasticsearch::HadoopInvocationOverride }
end
