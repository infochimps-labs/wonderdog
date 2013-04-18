#!/usr/bin/env ruby

require 'rubygems'
require 'configliere'
require 'gorillib'
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
# Parse command line;                                                                                                  #
# * Get logfile                                                                                                        #
########################################################################################################################

Settings.use :commandline
Settings.use :config_block
Settings.define :logfile
Settings.define :host
Settings.define :port
Settings.resolve!

########################################################################################################################
# Parse logfile, grab:                                                                                                 #
# *the timestamp                                                                                                       #
# *the index                                                                                                           #
# *the node                                                                                                            #
# *the type of search                                                                                                  #
# *the time in millisecond                                                                                             #
# *At least first 50 char of query                                                                                     #
########################################################################################################################

def parse_logline(line)

  if (line =~ %r{, source\[(.*)\], extra_source})
    query = $1
  else
    warn("couldn't parse line")
    return
  end

  line_parts = line.split("] [")
  front_meta = line_parts[0].split("][")
  back_meta = line_parts[2].split(", ")
  query_size = back_meta[4].length() - 2
  es_duration_size = back_meta[1].size

  query_hash = {}
  query_hash['node'] = line_parts[1]

  query_hash['original_timestamp'] = front_meta[0][1..-1]
  query_hash['flag'] = front_meta[1]
  query_hash['query_type'] = front_meta[2]

  query_hash['search_type'] = back_meta[2]
  query_hash['num_total_shards'] = back_meta[3]
  query_hash['es_duration'] = back_meta[1][12..es_duration_size-2]
  query_hash['index'] = back_meta[0].split("][")[0]
  query_hash['extra_source'] = back_meta[5]

  return query, query_hash

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
    original_timestamp = data['original_timestamp'].to_s
    es_duration = data['es_duration'].to_s
    new_duration = data['new_duration'].to_i.to_s
    node = data['node'].to_s
    index = data['index'].to_s
    if Random.rand() < 0.1
      header
    end
    #puts took.inspect, current_time.inspect, original_timestamp.inspect, es_duration.inspect, new_duration.inspect, node.inspect, index.inspect


    #puts "'current_time' \t 'original_timestamp' \t 'es_duration' \t 'new_duration' \t 'clock_time_duration' \t 'node'" +
    #         " \t 'index' \t 'extra_source'"

    #puts %w[current_time original_timestamp es_duration new_duration clock_time_duration node index].join("\t")
    puts [current_time, original_timestamp, es_duration, took, new_duration, node, index, query_fragment].join("\t")
  end
end

########################################################################################################################
# Execute slow query from log                                                                                          #
########################################################################################################################

def execute_query(query, data)
  if query.include? " " or query.index('(\\\'.*?\\\')').nil?
    if data['search_type'] == "search_type[QUERY_THEN_FETCH]"
      #puts "Executing query #{q_count}/#{@slowlog_lines.size}"
      data['new_timestamp'] = Time.now
      data['new_start_time'] = Time.now.to_f * 1000
      puts "curl -s -XGET #{Settings.host}:#{Settings.port}/#{data['index']}/_search/ -d '#{query}'"
      curl_result = `curl -s -XGET #{Settings.host}:#{Settings.port}/#{data['index']}/_search/ -d '#{query}'`
      data['new_end_time'] = Time.now.to_f * 1000
      data['new_duration'] = data['new_end_time'] - data['new_start_time']
      data = data.merge(JSON.parse(curl_result))
      output(query, data)
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



logfile = Settings.logfile
sl_regex = Regexp.new(('(slowlog\\.query)'), Regexp::IGNORECASE)
header
File.readlines(logfile).each do |line|
  if sl_regex.match(line)
    query, query_hash = parse_logline(line)
    execute_query(query, query_hash)
  end
end


