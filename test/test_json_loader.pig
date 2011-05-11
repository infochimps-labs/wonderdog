--
-- This tests the json indexer. Run in local mode with 'pig -x local test/test_json_loader.pig'
--
register target/wonderdog-1.0-SNAPSHOT.jar;
register /usr/local/share/elasticsearch/lib/elasticsearch-0.14.2.jar;
register /usr/local/share/elasticsearch/lib/jline-0.9.94.jar;
register /usr/local/share/elasticsearch/lib/jna-3.2.7.jar;
register /usr/local/share/elasticsearch/lib/log4j-1.2.15.jar;
register /usr/local/share/elasticsearch/lib/lucene-analyzers-3.0.3.jar;
register /usr/local/share/elasticsearch/lib/lucene-core-3.0.3.jar;
register /usr/local/share/elasticsearch/lib/lucene-fast-vector-highlighter-3.0.3.jar;
register /usr/local/share/elasticsearch/lib/lucene-highlighter-3.0.3.jar;
register /usr/local/share/elasticsearch/lib/lucene-memory-3.0.3.jar;
register /usr/local/share/elasticsearch/lib/lucene-queries-3.0.3.jar;        
        
%default INDEX 'foo_test'
%default OBJ   'foo'        

foo = LOAD 'test/foo.json' AS (data:chararray);
STORE foo INTO 'es://$INDEX/$OBJ' USING com.infochimps.elasticsearch.pig.ElasticSearchJsonIndex('-1', '10');
