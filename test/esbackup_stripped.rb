#!/usr/bin/env ruby

# Simple script to dump elasticsearch indexes as raw JSON

require 'tire'
require 'zlib'
require 'socket'
require 'pathname'
require 'configliere'
require 'multi_json'
#
#Settings.use :commandline
#def Settings.available_commands() %w[ backup restore ]                        ; end
#def Settings.usage()   'usage: esbackup command [ path ] [..--param=value..]' ; end
#Settings.description = <<-DESC.gsub(/^ {2}/, '').chomp
#  Simple backup and restore tool for Elasticsearch.
#
#  # Example backup command
#  $ esbackup backup -c localhost -p 9200 -b 100 -i my_index -q '{"query":{"term":{"field":"search_term"}}}'
#
#  # Example restore command
#  $ esbackup restore -c localhost -p 9200 -i my_index -m mapping_file.json
#
#  CAVEAT: Due to stupid fucking restrictions in the gem we use (tire), if you would like to use a dynamic query
#  when backing up you have to specify it like so: -q '{"query_method":["*args_to_method"]}'
#
#  Available commands:
##{Settings.available_commands.map{ |cmd| '    ' << cmd }.join("\n")}
#DESC
#Settings.define :host,       default: Socket.gethostname, flag: 'c', description: 'Host to connect to Elasticsearch'
#Settings.define :port,       default: 9200,               flag: 'p', description: 'Port to connect to Elasticsearch'
#Settings.define :batch_size, default: 10000,              flag: 'b', description: 'Batch size'
#Settings.define :index,                                   flag: 'i', description: 'Index to backup/restore', required: true
#Settings.define :mappings,                                flag: 'm', description: 'Dump mappings to or load mappings from provided file'
#Settings.define :query,                                   flag: 'q', description: 'A JSON hash containing a query to apply to the command (backup only)'
#Settings.define :dump_file,  default: nil,                flag: 'd', description: 'The name of the JSON dump file, if not passed the index name will be the dump files name'
#Settings.resolve!
#
#Tire::Configuration.url "http://#{Settings[:host]}:#{Settings[:port]}"

class ESBackup

  def initialize(output_dir, options = {})
    @output_dir   = output_dir || ''
    @index        = options[:index]
    @batch_size   = options[:batch_size].to_i
    @mapping_file = options[:mappings]
    @query        = MultiJson.load(options[:query]) rescue nil
    if options[:dump_file].nil?
      @dump_file    = @index
    else
      @dump_file = options[:dump_file]
    end
  end

  def dump_mapping
    index = Tire::Index.new @index
    File.open(@mapping_file, 'w'){ |f| f.puts index.mapping.to_json }
  end

  def fullpath dir
    basedir = dir.start_with?('/') ? dir : File.join(Dir.pwd, dir)
    FileUtils.mkdir_p(basedir)
    basedir
  end

  def gz_output
    File.join(fullpath(@output_dir), @index + '.gz')
  end

  def create_scanner
    scan_opts = { size: @batch_size }
    additional_query = @query
    Tire::Search::Scan.new(@index, scan_opts) do
      # This is fucking stupid; why people have to be cute and make everything DSL only
      # I'll never understand, but the person who wrote this gem has forced us to ONLY be able to
      # ask queries in this manner.
      query do
        additional_query.each_pair do |key, vals|
          case vals
            # Assuming here that you are only asking for one field at a time...this is getting hacky fast
            when Hash  then self.send(key.to_sym, *vals.to_a.flatten)
            when Array then self.send(key.to_sym, *vals)
          end
        end
      end if additional_query
    end
  end

  def run
    dump_mapping if @mapping_file
    gz = Zlib::GzipWriter.open gz_output
    count = 0
    create_scanner.each do |document|
      document.each do |record|
        json_doc = record.to_hash.except(:type, :_index, :_explanation, :_score, :_version, :highlight, :sort).to_json
        gz.puts json_doc
        count += 1
      end
    end
    gz.close
    puts "#{@index} backup complete. #{count} records written"
  end
end

class ESRestore

  def initialize(input, options = {})
    @index        = options[:index]
    @batch_size   = options[:batch_size].to_i
    @gz_input     = Zlib::GzipReader.open(input)
    @mapping_file = options[:mappings]
  end

  def create_index
    index   = Tire::Index.new @index
    options = @mapping_file ? { mappings: MultiJson.load(File.read(@mapping_file)) } : {}
    index.create(options) unless index.exists?
    index
  end

  def run
    reindex = create_index
    count, documents = 0, []
    @gz_input.each_line do |json|
      documents << MultiJson.load(json)
      count     += 1
      if count % @batch_size == 0
        reindex.bulk_create documents
        puts "#{count} records loaded"
        documents.clear
      end
    end
    @gz_input.close()
    reindex.bulk_create documents if not documents.empty?
    puts "#{@index} restore complete with #{count} records loaded"
  end
end

class ESDup

  def initialize(input, options = {})
    @index        = options[:index]
    @batch_size   = options[:batch_size].to_i
    @gz_input     = Zlib::GzipReader.open(input)
    @mapping_file = options[:mappings]
  end

  def create_index
    index   = Tire::Index.new @index
    options = @mapping_file ? { mappings: MultiJson.load(File.read(@mapping_file)) } : {}
    index.create(options) unless index.exists?
    index
  end

  def run
    reindex = create_index
    count, documents = 0, []
    @gz_input.each_line do |json|
      line = MultiJson.load(json)
      line.delete("_id")
      line.delete("id")
      documents << line
      count     += 1
      if count % @batch_size == 0
        reindex.bulk_create documents
        puts "#{count} records loaded"
        documents.clear
      end
    end
    @gz_input.close()
    reindex.bulk_create documents if not documents.empty?
    puts "#{@index} restore complete with #{count} records loaded"
  end
end
#
#case command = Settings.rest.shift.to_s.to_sym
#  when :restore then ESRestore.new(Settings.rest.shift, Settings.to_hash).run
#  when :backup  then ESBackup.new(Settings.rest.shift, Settings.to_hash).run
#  when :duplicate then ESDup.new(Settings.rest.shift, Settings.to_hash).run
#  else abort Settings.help("Must specify either backup, restore or duplicate.  Got <#{command}>")
#end

