#!/usr/bin/env ruby

# Simple script to dump elasticsearch indexes as raw JSON

require 'tire'
require 'zlib'
require 'socket'
require 'pathname'
require 'multi_json'

class ESBackup

  def initialize(output_dir, options = {})
    Tire::Configuration.url "http://#{options[:host]}:#{options[:port]}"
    @output_dir   = output_dir || ''
    @index        = options[:index]
    @batch_size   = options[:batch_size].to_i
    @mapping_file = options[:mappings]
    if options[:query].nil?
      @query      = nil
    else
      @query      = MultiJson.load(options[:query]) rescue nil
    end
    if options[:dump_file].nil?
      @dump_file  = @index
    else
      @dump_file  = options[:dump_file]
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
    Tire::Configuration.url "http://#{options[:host]}:#{options[:port]}"
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
    Tire::Configuration.url "http://#{options[:host]}:#{options[:port]}"
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

