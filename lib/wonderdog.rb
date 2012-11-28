require 'wukong-hadoop'

module Wukong
  
  # Wonderdog provides Java code that couples Hadoop streaming to
  # Wukong.  This module adds some overrides which enables the
  # <tt>wu-hadoop</tt> program to leverage this code.
  module Elasticsearch
  end
end

require 'wonderdog/configuration'
require 'wonderdog/hadoop_invocation_override'
