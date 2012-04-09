# Wonderdog

Wonderdog is a Hadoop interface to Elastic Search. While it is specifically intended for use with Apache Pig, it does include all the necessary Hadoop input and output formats for Elastic Search. That is, it's possible to skip Pig en
tirely and write custom Hadoop jobs if you prefer.

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
