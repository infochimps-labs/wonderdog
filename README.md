# Wonderdog

Wonderdog is a Hadoop interface to Elastic Search. While it is specifically intended for use with Apache Pig, it does include all the necessary Hadoop input and output formats for Elastic Search. That is, it's possible to skip Pig entirely and write custom Hadoop jobs if you prefer.

## Requirements

## Usage

### Using ElasticSearchStorage for Apache Pig

The most up-to-date (and simplest) way to store data into elasticsearch with hadoop is to use the Pig Store Function. You can write both delimited and json data to elasticsearch as well as read data from elasticsearch.

#### Storing tabular data:

This allows you to store tabular data (eg. tsv, csv) into elasticsearch.

```pig
%default ES_JAR_DIR '/usr/local/share/elasticsearch/lib'
%default INDEX      'ufo_sightings'
%default OBJ        'sighting'

register target/wonderdog*.jar;
register $ES_JAR_DIR/*.jar;

ufo_sightings = LOAD '/data/domestic/aliens/ufo_awesome.tsv' AS (sighted_at:long, reported_at:long, location:chararray, shape:chararray, duration:chararray, description:chararray);
STORE ufo_sightings INTO 'es://$INDEX/$OBJ?json=false&size=1000' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage();
```

Here the fields that you set in Pig (eg. 'sighted_at') are used as the field names when creating json records for elasticsearch.

#### Storing json data:

You can store json data just as easily.

```pig
ufo_sightings = LOAD '/data/domestic/aliens/ufo_awesome.tsv.json' AS (json_record:chararray);
STORE ufo_sightings INTO 'es://$INDEX/$OBJ?json=true&size=1000' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage();
```

#### Reading data:

Easy too.

```pig
-- dump some of the ufo sightings index based on free text query
alien_sightings = LOAD 'es://ufo_sightings/ufo_sightings?q=alien' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage() AS (doc_id:chararray, contents:chararray);
DUMP alien_sightings;
```

#### ElasticSearchStorage Constructor

The constructor to the UDF can take two arguments (in the following order):

* ```esConfig``` - The full path to where elasticsearch.yml lives on the machine launching the hadoop job
* ```esPlugins``` - The full path to where the elasticsearch plugins directory lives on the machine launching the hadoop job

#### Query Parameters

There are a few query paramaters available:

* ```json``` - (STORE only) When 'true' indicates to the StoreFunc that pre-rendered json records are being indexed. Default is false.
* ```size``` - When storing, this is used as the bulk request size (the number of records to stack up before indexing to elasticsearch). When loading, this is the number of records to fetch per request. Default 1000.
* ```q``` - (LOAD only) A free text query determining which records to load. If empty, matches all documents in the index.
* ```id``` - (STORE only) The name of the field to use as a document id. If blank (or -1) the documents are assumed to have no id and are assigned one by elasticsearch.
* ```tasks``` - (LOAD only) The number of map tasks to launch. Default 100.

Note that elasticsearch.yml and the plugins directory are distributed to every machine in the cluster automatically via hadoop's distributed cache mechanism.

### Native Hadoop TSV Loader

**Note**: the tsv loader is deprecated. Instead, use the ElasticSearchOutputFormat coupled with either Apache Pig storefunc (ElasticSearchIndex or ElasticSearchJsonIndex).

Once you've got a working set up you should be ready to launch your bulkload process. The best way to explain is with an example. Say you've got a tsv file of user records (name,login,email,description) and you want to index all the fields. Assuming you're going to write to an index called ```users``` with objects of type ```user``` (elasticsearch will create this object automatically the first time you upload one). The workflow is as follows:

* Create the ```users``` index:

```
bin/estool create --index users
```

* Upload the data

```
# Will only work if the hadoop elasticsearch processes can discover the running elasticsearch cluster
bin/wonderdog --rm --index_name=users --bulk_size=4096 --object_type=user --field_names=name,login,email,description --id_field=1 /hdfs/path/to/users.tsv /tmp/failed_records/users
```

Notice the output path. When the bulk indexing job runs it is possible for index requests to fail for various reasons (too much load, etc). In this case the documents that failed are simply written to the hdfs so they can be retried in a later job.

* Refresh Index

After the bulk load is finished you'll want to refresh the index so your documents will actually be searchable:

```
bin/estool refresh --index users
```

* Snapshot Index

You'll definitely want to do this after the bulk load finishes so you don't lose any data in case of cluster failure:

```
bin/estool snapshot --index users
```

* Bump the replicas for the index up to at least one.

```
bin/estool set_replication --index users --replicas=1
```

This will take a while to finish and the cluster health will show yellow until it does.

* Optimize the index

```
bin/estool optimize --index users -s 3
```

This will also take a while to finish.

#### TSV loader command-line options

* ```index_name``` - Index to write data to. It does not have to exist ahead of time
* ```object_type``` - Type of object to index. The mapping for this object does not have to exist ahead of time. Fields will be updated dynamically by elasticsearch.
* ```field_names``` - A comma separated list of field names describing the tsv record input
* ```id_field``` - Index of field to use as object id (counting from 0; default 1), use -1 if there is no id field
* ```bulk_size``` - Number of records per bulk request sent to elasticsearch cluster
* ```es_home``` - Path to elasticsearch installation, read from the ES_HOME environment variable if it's set
* ```es_config``` - Path to elasticsearch config file (@elasticsearch.yml@)
* ```rm``` - Remove existing output? (true or leave blank)
* ```hadoop_home``` - Path to hadoop installation, read from the HADOOP_HOME environment variable if it's set
* ```min_split_size``` - Min split size for maps

## Admin

There are a number of convenience commands in ```bin/estool```. Most of the common rest api operations have be mapped. Enumerating a few:

* Print status of all indices as a json hash to the terminal

```
# See everything (tmi)
bin/estool -c <elasticsearch_host> status
```

* Check cluster health (red,green,yellow,relocated shards, etc)

```
bin/estool -c <elasticsearch_host>  health
```

* Set replicas for an index

```
bin/estool set_replication -c <elasticsearch_host> --index <index_name> --replicas <num_replicas>
```

* Optimize an index

```
bin/estool optimize -c <elasticsearch_host> --index <index_name>
```

* Snapshot an index

```
bin/estool snapshot -c <elasticsearch_host> --index <index_name>
```

* Delete an index

```
bin/estool delete -c <elasticsearch_host> --index <index_name>
```


## Bulk Loading Tips for the Risk-seeking Dangermouse

The file examples/bulkload_pageviews.pig shows an example of bulk loading elasticsearch, including preparing the index.

### Elasticsearch Setup

Some tips for an industrial-strength cluster, assuming exclusive use of machines and no read load during the job:

* use multiple machines with a fair bit of ram (7+GB). Heap doesn't help too much for loading though, so you don't have to go nuts: we do fine with amazon m1.large's.
* Allocate a sizeable heap, setting min and max equal, and
  - turn `bootstrap.mlockall` on, and run `ulimit -l unlimited`.
  - For example, for a 3GB heap: `-Xmx3000m -Xms3000m -Delasticsearch.bootstrap.mlockall=true`
  - Never use a heap above 12GB or so, it's dangerous (STW compaction timeouts).
  - You've succeeded if the full heap size is resident on startup: that is, in htop both the VMEM and RSS are 3000 MB or so.
* temporarily increase the `index_buffer_size`, to say 40%.

### Further reading

* [Elasticsearch JVM Settings, explained](http://jprante.github.com/2012/11/28/Elasticsearch-Java-Virtual-Machine-settings-explained.html)

### Example of creating an index and mapping

Index:

    curl -XPUT ''http://localhost:9200/pageviews' -d '{"settings": {
      "index": { "number_of_shards": 12, "store.compress": { "stored": true, "tv": true } } }}'

    $ curl -XPUT 'http://localhost:9200/ufo_sightings/_settings?pretty=true'  -d '{"settings": {
      "index": { "number_of_shards": 12, "store.compress": { "stored": true, "tv": true } } }}'

Mapping (elasticsearch "type"):

    # Wikipedia Pageviews
    curl -XPUT ''http://localhost:9200/pageviews/pagehour/_mapping' -d '{
      "pagehour": { "_source": { "enabled" : true }, "properties" : {
        "page_id" :     { "type": "long",    "store": "yes" },
        "namespace":    { "type": "integer", "store": "yes" },
        "title":        { "type": "string",  "store": "yes" },
        "num_visitors": { "type": "long",    "store": "yes" },
        "date":         { "type": "integer", "store": "yes" },
        "time":         { "type": "long",    "store": "yes" },
        "ts":           { "type": "date",    "store": "yes" },
        "day_of_week":  { "type": "integer", "store": "yes" } } }}'

    $ curl -XPUT 'http://localhost:9200/ufo_sightings/sighting/_mapping' -d '{ "sighting": {
        "_source": { "enabled" : true },
        "properties" : {
          "sighted_at": { "type": "date", "store": "yes" },
		  "reported_at": { "type": "date", "store": "yes" },
          "shape": { "type": "string", "store": "yes" },
		  "duration": { "type": "string", "store": "yes" },
          "description": { "type": "string", "store": "yes" },
          "coordinates": { "type": "geo_point", "store": "yes" },
		  "location_str": { "type": "string", "store": "no" },
          "location": { "type": "object", "dynamic": false, "properties": {
            "place_id": { "type": "string", "store": "yes" },
			"place_type": { "type": "string", "store": "yes" },
            "city": { "type": "string", "store": "yes" },
			"county": { "type": "string", "store": "yes" },
            "state": { "type": "string", "store": "yes" },
			"country": { "type": "string", "store": "yes" } } }
        } } }'


### Temporary Bulk-load settings for an index

To prepare a database for bulk loading, the following settings may help. They are
*EXTREMELY* aggressive, and include knocking the replication factor back to 1 (zero replicas). One
false step and you've destroyed Tokyo.

Actually, you know what?  Never mind.  Don't apply these, they're too crazy.

    curl -XPUT 'http://localhost:9200/pageviews/_settings?pretty=true'  -d '{"index": {
      "number_of_replicas": 0, "refresh_interval": -1, "gateway.snapshot_interval": -1,
      "translog":     { "flush_threshold_ops": 50000, "flush_threshold_size": "200mb", "flush_threshold_period": "300s" },
      "merge.policy": { "max_merge_at_once": 30, "segments_per_tier": 30, "floor_segment": "10mb" },
      "store.compress": { "stored": true, "tv": true } } }'

To restore your settings, in case you didn't destroy Tokyo:

    curl -XPUT 'http://localhost:9200/pageviews/_settings?pretty=true'  -d ' {"index": {
      "number_of_replicas": 2, "refresh_interval": "60s", "gateway.snapshot_interval": "3600s",
      "translog": { "flush_threshold_ops": 5000, "flush_threshold_size": "200mb", "flush_threshold_period": "300s" },
      "merge.policy": { "max_merge_at_once": 10, "segments_per_tier": 10, "floor_segment": "10mb" },
      "store.compress": { "stored": true, "tv": true } } }'

If you did destroy your database, please send your resume to jobs@infochimps.com as you begin your
job hunt. It's the reformed sinner that makes the best missionary.


### Post-bulkrun maintenance

    es_index=pageviews ; ( for foo in _flush _refresh   '_optimize?max_num_segments=6&refresh=true&flush=true&wait_for_merge=true' '_gateway/snapshot'  ; do    echo "======= $foo" ; time curl -XPOST "http://localhost:9200/$es_index/$foo"  ; done ) &

### Full dump of cluster health

    es_index=pageviews ; es_node="projectes-elasticsearch-4"
    curl -XGET "http://localhost:9200/$es_index/_status?pretty=true"
    curl -XGET "http://localhost:9200/_cluster/state?pretty=true"
    curl -XGET  "http://localhost:9200/$es_index/_stats?pretty=true&merge=true&refresh=true&flush=true&warmer=true"
    curl -XGET "http://localhost:9200/_cluster/nodes/$es_node/stats?pretty=true&all=true"
    curl -XGET "http://localhost:9200/_cluster/nodes/$es_node?pretty=true&all=true"
    curl -XGET "http://localhost:9200/_cluster/health?pretty=true"
    curl -XGET "http://localhost:9200/$es_index/_search?pretty=true&limit=3"
    curl -XGET "http://localhost:9200/$es_index/_segments?pretty=true" | head -n 200

### Decommission nodes

Run this, excluding the decommissionable nodes from the list:

    curl -XPUT http://localhost:9200/pageviews/_settings -d '{
	  "index.routing.allocation.include.ironfan_name" :
	    "projectes-elasticsearch-0,projectes-elasticsearch-1,projectes-elasticsearch-2" }'
