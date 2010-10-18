require 'java'
require 'rubygems'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

Settings.define :es_home,    :default => '/usr/lib/elasticsearch',               :env_var => 'ES_HOME', :description => 'Path to elasticsearch installation directory'
Settings.define :es_plugins, :default => '/usr/lib/elasticsearch/plugins',                              :description => 'Path to plugins directory'
Settings.define :es_config,  :default => '/etc/elasticsearch/elasticsearch.yml', :env_var => 'ES_CONF', :description => 'Path to elasticsearch configuration'
Settings.resolve!
options = Settings.dup

# Set configuration properties but allow overwriting if it happens
java.lang.System.set_property("es.path.plugins", options.es_plugins)
java.lang.System.set_property("es.config", options.es_config)

# require all jars in es home
Dir["#{options.es_home}/lib/*.jar", "#{options.es_home}/lib/sigar/*.jar"].each{|jar| require jar}

module Wonderdog
  autoload :Client, 'wonderdog/client'
  autoload :Indexer,'wonderdog/indexer'
end
