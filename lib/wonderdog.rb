require 'wukong-hadoop'

module Wukong
  
  # Wonderdog provides Java code that couples Hadoop streaming to
  # Wukong.  This module adds some overrides which enables the
  # <tt>wu-hadoop</tt> program to leverage this code.
  module Elasticsearch
    include Plugin

    # Configure the given `settings` to be able to work with
    # Elasticsearch.
    #
    # @param [Configliere::Param] settings
    # @return [Configliere::Param] the newly configured settings
    def self.configure settings, program
      return unless program == 'wu-hadoop'
      settings.define(:es_tmp_dir,        :description => "Temporary directory on the HDFS to store job files while reading/writing to ElasticSearch", :default => "/user/#{ENV['USER']}/wukong", :wukong_hadoop => true)
      settings.define(:es_lib_dir,        :description => "Directory containing Elasticsearch, Wonderdog, and other support jars", :default => "/usr/lib/hadoop/lib", :wukong_hadoop => true)
      settings.define(:es_config,         :description => "Where to find configuration files detailing how to join an ElasticSearch cluster", :wukong_hadoop => true)
      settings.define(:es_input_splits,   :description => "Number of input splits to target when reading from ElasticSearch", :type => Integer, :wukong_hadoop => true)
      settings.define(:es_request_size,   :description => "Number of objects requested during each batch read from ElasticSearch", :type => Integer, :wukong_hadoop => true)
      settings.define(:es_scroll_timeout, :description => "Amount of time to wait on a scroll", :wukong_hadoop => true)
      settings.define(:es_index_field,    :description => "Field to use from each record to override the default index", :wukong_hadoop => true)
      settings.define(:es_mapping_field,  :description => "Field to use from each record to override the default mapping", :wukong_hadoop => true)
      settings.define(:es_id_field,       :description => "If this field is present in a record, make an update request, otherwise make a create request", :wukong_hadoop => true)
      settings.define(:es_bulk_size,      :description => "Number of requests to batch locally before making a request to ElasticSearch", :type => Integer, :wukong_hadoop => true)
      settings.define(:es_query,          :description => "Query to use when defining input splits for ElasticSearch input",    :wukong_hadoop => true)
      settings.define(:es_transport,      :description => "Use a transport client to an existing node instead of spinning up a new node", :default => true, type: :boolean, :wukong_hadoop => true)
      settings.define(:es_transport_host, :description => "Host of existing node for transport client", default: 'localhost', :wukong_hadoop => true)
      settings.define(:es_transport_port, :description => "Port of existing node for transport client", default: 9300, type: Integer, :wukong_hadoop => true)
    end

    # Boot Wonderdog with the given `settings` in the given `dir`.
    #
    # @param [Configliere::Param] settings
    # @param [String] root
    def self.boot settings, root
    end

  end
end

require 'wonderdog/hadoop_invocation_override'
require 'wonderdog/timestamp'
