#!/usr/bin/env jruby

require 'java'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

Settings.define :es_home,   :default => '/usr/lib/elasticsearch',               :env_var => 'ES_HOME', :description => 'Path to elasticsearch installation directory'
Settings.define :es_config, :default => '/etc/elasticsearch/elasticsearch.yml', :env_var => 'ES_CONF', :description => 'Path to elasticsearch configuration'
Settings.resolve!
options = Settings.dup

java.lang.System.set_property("es.config", options.es_config)

# require all jars in es home
Dir["#{options.es_home}/lib/*.jar", "#{options.es_home}/lib/sigar/*.jar"].each{|jar| require jar}

# This doesn't seem to work
# Dir["#{options.es_home}/plugins/*.zip"].each{|plugin| $CLASSPATH << plugin} 



# java imports
java_import 'org.elasticsearch.node.NodeBuilder'

# java_import 'org.elasticsearch.index.query.xcontent.FilterBuilders'
# java_import 'org.elasticsearch.index.query.xcontent.QueryBuilders'
# java_import 'org.elasticsearch.action.search.SearchType'

# java_import 'org.elasticsearch.index.query.QueryBuilder'
# java_import 'org.elasticsearch.index.query.xcontent.TermQueryBuilder'

def cluster_name client
  response = client.admin.cluster.prepare_state.execute.action_get
  response.cluster_name.to_string
end

node     = NodeBuilder.node_builder.node
client   = node.client


# search
# index = ["twitter2"].to_java(:string)
# term_query = QueryBuilders.term_query("multi", "test")
# p client.prepare_search(index).set_search_type(SearchType::DFS_QUERY_THEN_FETCH).set_query(term_query).set_from(0).set_size(60).set_explain(true).execute.action_get
# # do stuff ...

node.close
