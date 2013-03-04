SET mapred.map.tasks.speculative.execution false;

-- path to wikipedia pageviews data
%default PAGEVIEWS  's3n://bigdata.chimpy.us/data/results/wikipedia/full/pageviews/2008/03'
-- the target elasticsearch index and mapping ("type"). Will be created, though you
-- should do it yourself first instead as shown below.
%default INDEX      'pageviews'
%default OBJ        'pagehour'
-- path to elasticsearch jars
%default ES_JAR_DIR '/usr/local/share/elasticsearch/lib'
-- Batch size for loading
%default BATCHSIZE  '10000'

-- Example of bulk loading. This will easily load more than a billion documents
-- into a large cluster. We recommend using Ironfan to set your junk up.
--
-- Preparation:
--
-- Create the index:
--
--    curl -XPUT 'http://projectes-elasticsearch-0.test.chimpy.us:9200/pageviews' -d '{"settings": { "index": {
--      "number_of_shards": 12, "number_of_replicas": 0, "store.compress": { "stored": true, "tv": true } } }}'
--
-- Define the elasticsearch mapping (type):
--
--    curl -XPUT 'http://projectes-elasticsearch-0.test.chimpy.us:9200/pageviews/pagehour/_mapping' -d '{
--      "pagehour": {
--        "_source": { "enabled" : true },
--        "properties" : {
--          "page_id" :     { "type": "long",    "store": "yes" },
--          "namespace":    { "type": "integer", "store": "yes" },
--          "title":        { "type": "string",  "store": "yes" },
--          "num_visitors": { "type": "long",    "store": "yes" },
--          "date":         { "type": "integer", "store": "yes" },
--          "time":         { "type": "long",    "store": "yes" },
--          "ts":           { "type": "date",    "store": "yes" },
--          "day_of_week":  { "type": "integer", "store": "yes" } } }}'
--
-- For best results, see the 'Tips for Bulk Loading' in the README.
--

-- Always disable speculative execution when loading into a database
set mapred.map.tasks.speculative.execution false
-- Don't re-use JVM: logging gets angry
set mapred.job.reuse.jvm.num.tasks         1
-- Use large file sizes; setup/teardown time for leaving the cluster is worse
-- than non-local map tasks
set mapred.min.split.size                  3000MB
set pig.maxCombinedSplitSize               2000MB
set pig.splitCombination                   true

register ./target/wonderdog*.jar;
register $ES_JAR_DIR/*.jar;

pageviews = LOAD '$PAGEVIEWS' AS (
        page_id:long, namespace:int, title:chararray,
        num_visitors:long, date:int, time:long,
        epoch_time:long, day_of_week:int);
pageviews_fixed = FOREACH pageviews GENERATE
        page_id, namespace, title,
        num_visitors, date, time,
        epoch_time * 1000L AS ts, day_of_week;

STORE pageviews_fixed INTO 'es://$INDEX/$OBJ?json=false&size=$BATCHSIZE' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage();

-- -- To instead dump the JSON data to disk (needs Pig 0.10+)
-- set dfs.replication                        2
-- %default OUTDUMP    '$PAGEVIEWS.json'
-- rmf                         $OUTDUMP
-- STORE pageviews_fixed INTO '$OUTDUMP' USING JsonStorage();
