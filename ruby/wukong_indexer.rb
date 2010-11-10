#!/usr/bin/env jruby

this = File.expand_path(__FILE__)
$: << File.expand_path(this+'/../../lib')
$: << '/usr/lib/jruby/lib/ruby/gems/1.8/gems/extlib-0.9.15/lib'
$: << '/usr/lib/jruby/lib/ruby/gems/1.8/gems/htmlentities-4.2.1/lib'
$: << '/usr/lib/jruby/lib/ruby/gems/1.8/gems/addressable-2.2.2/lib'
$: << '/usr/lib/jruby/lib/ruby/gems/1.8/gems/json-1.4.6-java/lib'

#
# Need to make sure chef has installed jruby and the above gems for this to work
#
require 'wonderdog'
require 'wukong'
require 'wukong/encoding'
require 'wuclan/twitter' ; include Wuclan::Twitter


#
# The process here is as follows:
#
# On every machine in the elastic search cluster instantiate an es_server using
# chef. These are the nodes that will be allocated data and do the actual
# indexing. Create a powerful hadoop cluster capable of engorging all the
# tweets. These machines will run as many map tasks as necessary, and thus, as
# many es_client nodes as necessary, to parse out the tweets and convert them
# into an indexable binary format. This binary form is sent, in batches to the
# es_cluster and indexed.
#

Settings.define :buffered_docs, :default => 10000, :type => Integer, :description => "Number of documents to keep in memory before creating a bulk indexing request"

#
# Responsible for eating tweet objects from hdfs (or possibly s3) and converting
# them, as efficiently as possible, to binary representations for indexing by
# the elastic search cluster. The embedded clients created, on per map task, do
# not do any indexing themselves.
#
class ParseMapper < Wukong::Streamer::StructStreamer
  attr_accessor :aggregator

  #
  # Called once per map task, thus we will have as many es_clients as map tasks
  #
  def initialize *args
    super(*args)
    @indexer    = Wonderdog::Parser.new("foo", "tweet")
    @aggregator = []
  end

  #
  # Aggregate up to 'buffered_docs' in memory before calling batch process.
  #
  def process tweet, *_
    @aggregator << tweet.to_hash
    if @aggregator.size == options.buffered_docs
      response = @indexer.index_array(@aggregator)
      raise "Your motherfucking bulk request failed." if response.has_failures?
      warn("Success")
      @aggregator = []
    end
  end

  #
  # Index any remaining tweets.
  #
  def after_stream *args
    super(*args)
    @indexer.index_array(@aggregator) unless @aggregator.empty?
    @indexer.refresh_indices
  end

end

Wukong::Script.new(ParseMapper, nil).run
