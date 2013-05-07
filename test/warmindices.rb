#!/usr/bin/env ruby

require 'wukong'
require 'httparty'
require 'yaml'

class WarmIndices < Wukong::Runner
  include Wukong::Plugin

  description <<-DESC.gsub(/^ {4}/, '').strip
    Run this script to warm all indices in an Elasticsearch cluster.
    This relies on the mapping settings provided in

      config/mappings.yml

    Run with the --debug (-d) option to see pretty printed output without
    actually calling the Elasticsearch API.
  DESC

  class << self
    def configure(env, prog)
      env.define :host,    default: 'localhost', flag: 'c', description: 'The hostname of the Elasticsearch cluster'
      env.define :port,    default: 9200,        flag: 'p', description: 'The port number that Elasticsearch opened for web requests'
      env.define :debug,   default: false,       flag: 'd', description: 'Run in debug mode'
      env.define :indices, type:    Array,       flag: 'i', description: 'Comma-separated list of indices to warm. Defaults to all'
    end
  end

  def mappings
    @mappings ||= YAML.load File.read(File.expand_path('../../config/mappings.yml', __FILE__))
  end

  def indices
    settings.indices || mappings.keys
  end

  def warmer_id index
    "#{index}_warmer"
  end

  def warmer_uri index
    "http://#{settings.host}:#{settings.port}/#{index}/_warmer/#{warmer_id(index)}?"
  end

  def sorted_query fields
    { query: { match_all: {} }, sort: fields }
  end

  def run
    indices.each do |index|
      fields = mappings[index]['properties'].keys rescue abort("The provided index <#{index}> was not found in config/mappings.yml")
      uri    = warmer_uri(index)
      body   = sorted_query(fields)
      puts "Creating warmer for index #{index}"
      if settings.debug
        puts MultiJson.encode(body)
      else
        res = HTTParty.put(uri, body: MultiJson.encode(body))
        puts res.response
      end
    end
  end
end

WarmIndices.run
