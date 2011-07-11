--
-- This tests the tsv indexer. Run in local mode with 'pig -x local test/test_tsv_loader.pig'
--
register '/usr/local/share/elasticsearch/lib/elasticsearch-0.16.0.jar';
register '/usr/local/share/elasticsearch/lib/jline-0.9.94.jar';
register '/usr/local/share/elasticsearch/lib/jna-3.2.7.jar';
register '/usr/local/share/elasticsearch/lib/log4j-1.2.15.jar';
register '/usr/local/share/elasticsearch/lib/lucene-analyzers-3.1.0.jar';
register '/usr/local/share/elasticsearch/lib/lucene-core-3.1.0.jar';
register '/usr/local/share/elasticsearch/lib/lucene-highlighter-3.1.0.jar';
register '/usr/local/share/elasticsearch/lib/lucene-memory-3.1.0.jar';
register '/usr/local/share/elasticsearch/lib/lucene-queries-3.1.0.jar';
register target/wonderdog-1.0-SNAPSHOT.jar;
        
%default INDEX 'foo_test'
%default OBJ   'foo'        

foo = LOAD 'test/foo.tsv' AS (character:chararray, value:int);

STORE foo INTO 'es://$INDEX/$OBJ?json=false&size=1' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage();
