--
-- This tests the tsv indexer. Run in local mode with 'pig -x local test/test_tsv_loader.pig'
--
%default ES_JAR_DIR '/usr/local/Cellar/elasticsearch/0.18.7/libexec'
%default ES_YAML    '/usr/local/Cellar/elasticsearch/0.18.7/config/elasticsearch.yml'
%default PLUGINS    '/usr/local/Cellar/elasticsearch/0.18.7/plugins'

%default INDEX      'foo_test'
%default OBJ        'foo'        

register $ES_JAR_DIR/*.jar;
register target/wonderdog*.jar;

foo = LOAD 'test/foo.tsv' AS (character:chararray, value:int);

STORE foo INTO 'es://$INDEX/$OBJ?json=false&size=1' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage('$ES_YAML', '$PLUGINS');
