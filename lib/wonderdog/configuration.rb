module Wukong
  module Elasticsearch
    Configuration = Configliere::Param.new unless defined?(Configuration)

    Configuration.define(:tmp_dir,        :description => "Temporary directory on the HDFS to store job files while reading/writing to ElasticSearch", :default => "/user/#{ENV['USER']}/wukong", :es => true)
    Configuration.define(:config,         :description => "Where to find configuration files detailing how to join an ElasticSearch cluster", :es => true)
    Configuration.define(:input_splits,   :description => "Number of input splits to target when reading from ElasticSearch", :type => Integer, :es => true)
    Configuration.define(:request_size,   :description => "Number of objects requested during each batch read from ElasticSearch", :type => Integer, :es => true)
    Configuration.define(:scroll_timeout, :description => "Amount of time to wait on a scroll", :es => true)
    Configuration.define(:index_field,    :description => "Field to use from each record to override the default index", :es => true)
    Configuration.define(:type_field,     :description => "Field to use from each record to override the default type", :es => true)
    Configuration.define(:id_field,       :description => "If this field is present in a record, make an update request, otherwise make a create request", :es => true)
    Configuration.define(:bulk_size,      :description => "Number of requests to batch locally before making a request to ElasticSearch", :type => Integer, :es => true)
    
  end
end
