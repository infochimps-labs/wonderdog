#!/usr/bin/env ruby

require 'rubygems'
require 'json'
require 'time'


#require 'random'

########################################################################################################################
# This program is designed to read an elasticsearch log file and return                                                #
# information about how long a slow process took, run the query, and                                                   #
# return information about how long it took to run the query again.                                                    #
# Example command:                                                                                                     #
#   ruby ./replay.rb --logfile=/var/log/elasticsearch/patrick.log --port=9200 --host=localhost                    #
########################################################################################################################

########################################################################################################################
# Global variables for storing metadata                                                                                #
########################################################################################################################
@slowlog_lines = []
@metadata_hash = {}

########################################################################################################################
# Parse logfile, grab:                                                                                                 #
# *the timestamp                                                                                                       #
# *the index                                                                                                           #
# *the node                                                                                                            #
# *the type of search                                                                                                  #
# *the time in millisecond                                                                                             #
# *At least first 50 char of query                                                                                     #
########################################################################################################################

class ParseMetaData
  attr_accessor :metaData

  def initialize(metaString, metaArray = [])
    @metaString = metaString
    @metaArray = metaArray
    @metaData = {}
    @bracket_pairs = get_bracket_pair_indexes
  end

  def get_left_bracket_indexes
    @metaString.enum_for(:scan, Regexp.new('\[')).map {Regexp.last_match.begin(0)}
  end

  def get_right_bracket_indexes
    @metaString.enum_for(:scan, Regexp.new('\]')).map {Regexp.last_match.begin(0)}
  end

  def get_bracket_pair_indexes
    get_left_bracket_indexes.zip(get_right_bracket_indexes)
  end

  def get_query
    startInd = @metaString.enum_for(:scan, Regexp.new(' source\[')).map {Regexp.last_match.begin(0)+8}
    endInd = @metaString.enum_for(:scan, Regexp.new('_source\[')).map {Regexp.last_match.begin(0)-9}
    @metaData["query"] = @metaString[startInd[0]..endInd[0]]
  end

  def find_meta_data(meta)
    start = @metaString.enum_for(:scan, Regexp.new(meta)).map {Regexp.last_match.begin(0) + meta.size}
    index = get_left_bracket_indexes.index(start[0])
    unless index.nil?
      bracket_pair = @bracket_pairs[index]
      #puts @metaString[bracket_pair[0]+1..bracket_pair[1]-1].inspect
      @metaData[meta] = @metaString[bracket_pair[0]+1..bracket_pair[1]-1]
    end
  end

  def get_extra_meta_data
    @metaArray.each do |meta|
      find_meta_data(meta)
    end
  end

  def get_basic_meta_data
    #FIXME! Make this dynamic and depended on the first four [] to contain the same things everytime
    @metaData["timestamp"] = @metaString[@bracket_pairs[0][0]+1..@bracket_pairs[0][1]-1]
    @metaData["node"] = @metaString[@bracket_pairs[3][0]+1..@bracket_pairs[3][1]-1]
    @metaData["index"] = @metaString[@bracket_pairs[4][0]+1..@bracket_pairs[4][1]-1]
  end

  def get_meta_data
    get_basic_meta_data
    get_query
    unless @metaArray == []
      get_extra_meta_data
    end
  end
end

def parse_logline(line, metaArray)

  if (line =~ %r{, source\[(.*)\], extra_source})
    query = $1
  else
    warn("couldn't parse line")
    return
  end

  #puts line
  parser = ParseMetaData.new(line, metaArray)
  parser.get_meta_data

  return parser.metaData["query"], parser.metaData
end

########################################################################################################################
# Return the following info to stdout as tab delimited:                                                                #
# Current time                                                                                                         #
# Original timestamp                                                                                                   #
# Duration of query in log                                                                                             #
# Duration of re-ran query according to elastic search                                                                 #
# Duration of re-ran query according to the wall clock                                                                 #
# The meta captured from the logfile                                                                                   #
# A snippet of query                                                                                                   #
# Extra source data from logfile                                                                                       #
########################################################################################################################
class Replay

  def initialize(logfile, host, port, preference)
    @logfile = logfile
    @host = host
    @port = port
    @preference = preference
  end

  def header()
    puts "\n"
    puts %w[current_timestamp original_timestamp es_duration(ms) new_duration(ms) clock_time_duration(ms) node index query_fragment].join("\t")
  end

  def output(query, data, malformed=false)
    query_fragment = query[0..49]
    if malformed
      puts "malformed"
      puts query_fragment
    else
      took = data['took'].to_s
      current_time = data['new_timestamp'].to_s
      original_timestamp = data['timestamp'].to_s
      es_duration = data['original_dur'].to_s
      new_duration = data['new_duration'].to_i.to_s
      node = data['node'].to_s
      index = data['index'].to_s
      if Random.rand() < 0.1
        header
      end
      puts [current_time, original_timestamp, es_duration, took, new_duration, node, index, query_fragment].join("\t")
    end
  end

  def build_curl_command_string(query, data)
    base_uri = "''#{@host}:#{@port}/#{data['index']}/_search/"
    if @preference[0]
      base_uri.concat("preference=#{@preference[1]}''")
    end
    curl_command = "curl -s -XGET ".concat(base_uri)
    puts curl_command.concat("' -d '#{query}''")
  end

########################################################################################################################
# Execute slow query from log                                                                                          #
########################################################################################################################

  def execute_query(query, data)
    if query.include? " " or query.index('(\\\'.*?\\\')').nil?
      if data['search_type'] == "QUERY_THEN_FETCH"
        data['new_timestamp'] = Time.now
        data['new_start_time'] = Time.now.to_f * 1000
        #puts "curl -s -XGET #{Settings.host}:#{Settings.port}/#{data['index']}/_search/ -d '#{query}'"
        build_curl_command_string(query, data)
        #curl_result = `curl -s -XGET '#{@host}:#{@port}/#{data['index']}/_search/' -d '#{query}'`
        #data['new_end_time'] = Time.now.to_f * 1000
        #data['new_duration'] = data['new_end_time'] - data['new_start_time']
        #data['original_dur'] = data['took']
        #data = data.merge(JSON.parse(curl_result))
        #output(query, data)
      else
        puts "error don't know search type, please throw an exception here"
      end
    else
      puts "malformed query string"
      puts query
      output(query, data, malformed=true)
    end
  end

########################################################################################################################
# MAIN                                                                                                                 #
########################################################################################################################

  def run
    sl_regex = Regexp.new(('(slowlog\\.query)'), Regexp::IGNORECASE)
    metaArray = %w[took took_millis types search_type total_shards]
    header
    File.readlines(@logfile).each do |line|
      if sl_regex.match(line)
        query, query_hash = parse_logline(line, metaArray)
        execute_query(query, query_hash)
      end
    end
  end
end


