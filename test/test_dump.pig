--
-- This tests loading data from elasticsearch
--

%default ES_JAR_DIR '/usr/local/Cellar/elasticsearch/0.18.7/libexec'
%default ES_YAML    '/usr/local/Cellar/elasticsearch/0.18.7/config/elasticsearch.yml'
%default PLUGINS    '/usr/local/Cellar/elasticsearch/0.18.7/plugins'

%default INDEX      'foo_test'
%default OBJ        'foo'        

register $ES_JAR_DIR/*.jar;
register target/wonderdog*.jar;

--
-- Will load the data as (doc_id, contents) tuples where the contents is the original json source from elasticsearch
--
foo = LOAD 'es://$INDEX/$OBJ' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage('$ES_YAML', '$PLUGINS') AS (doc_id:chararray, contents:chararray);
DUMP foo;
