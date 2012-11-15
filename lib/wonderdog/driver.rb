require 'shellwords'
require 'uri'

module Wukong
  module Elasticsearch
    class Driver

      STREAMING_INPUT_FORMAT  = "com.infochimps.elasticsearch.ElasticSearchStreamingInputFormat"
      STREAMING_OUTPUT_FORMAT = "com.infochimps.elasticsearch.ElasticSearchStreamingOutputFormat"

      class IndexAndType
        attr_reader :index, :type
        def initialize uri
          self.uri = uri
        end
        def self.uri= u
          begin
            @uri   = URI.parse(u)
            path   = File.join(@uri.host, @uri.path)
            @index = path.split('/')[0]
            @type  = path.split('/')[1]
          rescue URI::InvalidURIError => e
            raise Wukong::Error.new("Could not parse '#{u}' as an ElasticSearch /index/type specification")
          end
        end
      end
      
      attr_accessor :settings

      def self.run(settings, *extra_args)
        begin
          new(settings, *extra_args).run!
        rescue Wukong::Error => e
          $stderr.puts e.message
          exit(127)
        end
      end

      def initialize(settings, *args)
        @settings = settings
      end

      def read?
        settings[:input] && settings[:input] =~ %r!^es://!
      end

      def write?
        settings[:output] && settings[:output] =~ %r!^es://!
      end
      
      def input
        @input ||= IndexAndType.new(settings[:input])
      end

      def output
        @output ||= IndexAndType.new(settings[:output])
      end
      
      def run!
        # puts wu_hadoop_command
        exec wu_hadoop_command
      end

      def wu_hadoop_command
        [
         'wu-hadoop',
         wu_hadoop_input,
         wu_hadoop_output,
         java_opts,
         non_es_params,
         args
        ].flatten.compact.join(' ')
      end

      def args
        settings.rest
      end
      
      def wu_hadoop_input
        case
        when settings[:input] && read?
          "--input=#{tmp_filename(input)} --input_format=#{STREAMING_INPUT_FORMAT}"
        when settings[:input]
          "--input=#{settings[:input]}"
        end
      end

      def wu_hadoop_output
        case
        when settings[:output] && write?
          "--output=#{tmp_filename(output)} --output_format=#{STREAMING_OUTPUT_FORMAT}"
        when settings[:output]
          "--output=#{settings[:output]}"
        end
      end

      def non_es_params
        settings.reject{ |param, val| settings.definition_of(param, :es) }.map{ |param,val| "--#{param}=#{val}" }
      end

      def tmp_filename io
        cleaner  = %r{[^\w/\.\-\+]+}
        io_part  = [io.index, io.type].compact.map { |s| s.gsub(cleaner, '') }.join('/')
        job_part = settings.rest.map { |s| File.basename(s, '.rb').gsub(cleaner,'') }.join('---')
        File.join(settings[:tmp_dir], io_part, job_part, Time.now.strftime("%Y-%m-%d-%H-%M-%S"))
      end

      def java_opts
        return unless read? || write?
        
        opts = [].tap do |o|
          o << java_opt('es.config', settings[:config]) if settings[:config]
          
          if read?
            o << java_opt('elasticsearch.input.index',          input.index)
            o << java_opt('elasticsearch.input.type',           input.type)
            o << java_opt('elasticsearch.input.splits',         settings[:input_splits])   if settings[:input_splits]
            o << java_opt('elasticsearch.input.query',          settings[:query])          if settings[:query]
            o << java_opt('elasticsearch.input.request_size',   settings[:request_size])   if settings[:request_size]
            o << java_opt('elasticsearch.input.scroll_timeout', settings[:scroll_timeout]) if settings[:scroll_timeout]
          end

          if write?
            o << java_opt('elasticsearch.output.index',       output.index)
            o << java_opt('elasticsearch.output.type',        output.type)
            o << java_opt('elasticsearch.output.index.field', settings[:index_field]) if settings[:index_field]
            o << java_opt('elasticsearch.output.type.field',  settings[:type_field])  if settings[:type_field]
            o << java_opt('elasticsearch.output.id.field',    settings[:id_field])    if settings[:id_field]
            o << java_opt('elasticsearch.output.bulk_size',   settings[:bulk_size])   if settings[:bulk_size]
          end
        end
        return if opts.empty?
        joined = opts.join(' ')
        "--java_opts='#{joined}'"
      end

      def java_opt name, value
        return if value.nil?
        "-D #{name}=#{Shellwords.escape(value.to_s)}"
      end
    end
  end
end

      
