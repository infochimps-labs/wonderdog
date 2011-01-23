#!/usr/bin/env ruby

require 'rubygems'
require 'rubberband'
require 'fileutils'
require 'configliere'
Settings.use :commandline

#
# Estrus -- an alluringly primitive Elasticsearch stress-testing tool
#
# Example usage:
#
#   ~/ics/backend/wonderdog/ruby/estrus.rb --queries=100 --output_dir=~/ics/backend/estrus_data
#
# Output:
#
#   idx  datetime  secs   msec/query   hits  shards_successful  index nodename  query_term
#
# Setup
#
#   sudo apt-get install -y libcurl4-dev wamerican-large
#   sudo gem install rubberband configliere

# ,tweet-2010q1
Settings.define :words_file,   :default => "/usr/share/dict/words", :description => "Flat file with words to use"
Settings.define :offset_start, :default => 50_000,                  :description => "Where to start reading words", :type => Integer
Settings.define :offset_scale, :default => 100,                     :description => "How far in the file to range", :type => Integer
Settings.define :queries,      :default => 10,                      :description => "Number of queries to run",     :type => Integer
Settings.define :es_indexes,   :default => 'tweet-2009q3pre,tweet-2009q4,tweet-201004,tweet-201005,tweet-201006',          :description => "Elasticsearch index to query against", :type => Array
Settings.define :output_dir,   :default => nil,                     :description => "If given, the output is directed to a file named :output_dir/{date}/es-{datetime}-{comment}-{hostname}.tsv"
Settings.define :comment,      :default => nil,                     :description => "If given, it is included in the filename"
Settings.resolve!

HOSTNAME = ENV['HOSTNAME'] || `hostname`.chomp
NODENAME = File.read('/etc/node_name').chomp rescue HOSTNAME
CLIENTS = Settings.es_indexes.inject([]){|clients, index| clients << [index, ElasticSearch.new("#{HOSTNAME}:9200", :index => index, :type => "tweet")] ; clients }

class StressTester
  attr_accessor :started_at
  def initialize
    self.started_at = Time.now.utc
  end

  def words_file &block
    File.open(Settings.words_file, &block)
  end

  def random_offset
    Settings.offset_start + rand(1000)*Settings.offset_scale rescue nil
  end

  def output_file
    return @output_file if @output_file
    return $stdout if Settings.output_dir.blank?
    datehr   = started_at.strftime("%Y%m%d%H")
    datetime = started_at.to_flat
    output_filename = File.expand_path(File.join(Settings.output_dir, datehr,
        ["es", datetime, NODENAME, Settings.comment].compact.join('-')+".tsv"))
    FileUtils.mkdir_p(File.dirname(output_filename))
    @output_file = File.open(output_filename, "a")
  end

  def dump *args
    output_file << args.join("\t")+"\n"
  end

  def each_word &block
    words_file do |words_file|
      random_offset.times{ words_file.readline }
      loop do
        word = words_file.readline.chomp rescue nil
        break unless word
        next if word =~ /\W/
        yield word
      end
    end
  end
end

class Time ; def to_flat() strftime("%Y%m%d%H%M%S"); end ; end
class Array ; def random() self[rand(length)] ; end ; end

tester = StressTester.new
n_queries_executed = 0
tester.each_word do |query_string|
  index, client = CLIENTS.random

  result  = client.search "text:#{query_string}"
  elapsed = Time.now.utc - tester.started_at
  n_queries_executed += 1
  tester.dump(
    n_queries_executed, Time.now.utc.to_flat, "%7.1f"%elapsed,
    "%7.1f"%( 1000 * elapsed / n_queries_executed.to_f ),
    result.total_entries, result._shards['successful'],
    index, NODENAME,
    query_string)
  $stderr.puts(n_queries_executed) if n_queries_executed % 20 == 0
  break if n_queries_executed >= Settings.queries
end

# query_string = 'verizon'
# CLIENTS.each do |index,client|
#   result  = client.search "text:#{query_string}"
#   elapsed = Time.now.utc - tester.started_at
#   n_queries_executed += 1
#   tester.dump(
#     n_queries_executed, Time.now.utc.to_flat, "%7.1f"%elapsed,
#     "%7.1f"%( 1000 * elapsed / n_queries_executed.to_f ),
#     result.total_entries, result._shards['successful'],
#     index, NODENAME,
#     query_string)
#
# end




#
# TODO: monkeypatch rubberband to use keepalives:
#
      # def connect!
      #   unless defined?(@@patron_session)
      #     @@patron_session = Patron::Session.new
      #     @session = @@patron_session
      #     @session.base_url = @server
      #     @session.timeout = @options[:timeout]
      #     @session.headers['User-Agent'] = 'ElasticSearch.rb v0.1'
      #     @session.headers['Connection'] = 'Keep-Alive'
      #   else
      #     @session = @@patron_session
      #   end
      #   @request_count = 1
      # end

