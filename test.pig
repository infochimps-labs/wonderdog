register build/wonderdog.jar
register /usr/local/share/elasticsearch/lib/elasticsearch-0.14.2.jar
register /usr/local/share/elasticsearch/lib/jline-0.9.94.jar
register /usr/local/share/elasticsearch/lib/jna-3.2.7.jar
register /usr/local/share/elasticsearch/lib/log4j-1.2.15.jar
register /usr/local/share/elasticsearch/lib/lucene-analyzers-3.0.3.jar
register /usr/local/share/elasticsearch/lib/lucene-core-3.0.3.jar
register /usr/local/share/elasticsearch/lib/lucene-fast-vector-highlighter-3.0.3.jar
register /usr/local/share/elasticsearch/lib/lucene-highlighter-3.0.3.jar
register /usr/local/share/elasticsearch/lib/lucene-memory-3.0.3.jar
register /usr/local/share/elasticsearch/lib/lucene-queries-3.0.3.jar        
        
%default INDEX 'ufo_sightings'
%default OBJ   'ufo_sighting'        

ufo_sightings = LOAD '/data/domestic/aliens/ufo_awesome.tsv' AS (sighted_at:long, reported_at:long, location:chararray, shape:chararray, duration:chararray, description:chararray);
STORE ufo_sightings INTO 'es://$INDEX/$OBJ' USING com.infochimps.elasticsearch.pig.ElasticSearchIndex('-1', '1000');
