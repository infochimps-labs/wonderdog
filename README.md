# Wonderdog

Wonderdog makes ElasticSearch easier to connect with Hadoop.  It
provides a few kinds of functionality:

* <a href="#hadoop">Java InputFormat and OutputFormat classes</a> that you can use in your own Hadoop MapReduce jobs
* A <a href="#wukong">Wukong plugin</a> which makes these InputFormat and OutputFormat classes easy to use from [Wukong](http://github.com/infochimps-labs/wukong)
* <a href="#pig">Java functions for Pig</a> `LOAD` from and `STORE` into ElasticSearch
* some <a href="#utilities">command-line utilities</a> for interacting with ElasticSearch

<a name="hadoop">
# Hadoop MapReduce

Wonderdog provides InputFormat and OutputFormat classes that can be
used in your own custom Hadoop MapReduce jobs.

* com.infochimps.elasticsearch.ElasticSearchInputFormat
* com.infochimps.elasticsearch.ElasticSearchOutputFormat
* com.infochimps.elasticsearch.ElasticSearchStreamingInputFormat
* com.infochimps.elasticsearch.ElasticSearchStreamingOutputFormat

These classes come in streaming (for the old `mapred` API) and
non-streaming (for the new `mapreduce` API) flavors.

## Installing Wonderdog

To use these classes, you'll need to declare a dependency on Wonderdog
in your project's `pom.xml`:

```xml
<project>
  ...
  <dependencies>
    <dependency>
      <groupId>com.infochimps</groupId>
      <artifactId>elasticsearch</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>
    ...
  </dependencies>
  ...
</project>
```

Now when you build your code, it will include the Wonderdog
InputFormat and OutputFormat classes you need.

TBD:
* examples of using these classes in your on MapReduce jobs
* examples of launching such a job from the command-line

<a name="wukong">
# Wukong

Wonderdog also provides a
[Wukong](http://github.com/infochimps-labs/wukong) plugin to make it
easy to use the InputFormat and OutputFormat classes.

## Installing Wonderdog

Ensure that Wonderdog is in your project's Gemfile:

```ruby
# in Gemfile
gem 'wonderdog', git: 'https://github.com/infochimps-labs/wonderdog'
```

You'll have to require Wonderdog at the top of your job
```ruby
# in my_elasticsearch_job.rb

require 'wukong'
require 'wonderdog'
 
Wukong.dataflow(:mapper) do
 ...
end
 
Wukong.dataflow(:reducer) do
 ...
end
```

If you are running a
[deploy pack](http://github.com/infochimps-labs/wukong-deploy) then
you may want to require Wonderdog at the top-level of your deploy
pack by creating an initializer:

```ruby
# in config/initializers/plugins.rb
require 'wonderdog'
```

## Using Wonderdog From Wukong

Wukong uses
[Wukong-Hadoop](http://github.com/infochimps-labs/wukong-hadoop) to
provide the basic functionality of connecting Wukong to Hadoop.
Wonderdog modifies this connection by adjusting the command-lines
passed to the `hadoop` program so that the correct input and output
formats are used.

A "normal" Hadoop streaming job launched by Wukong-Hadoop might look
like this:

```
$ wu hadoop my_job.rb --input=/some/hdfs/input/path --output=/some/hdfs/output/path
```

Assuming you've correctly installed Wonderdog into your job or deploy
pack, you should be able to invoke Wonderdog's core classes by
changing the URI for `input` or `output` to use a scheme of `es`.  The
"host" of the URI is the index in ElasticSearch and the "path" the
type.

### Embedded vs. Transport Nodes

Wonderdog provides two different ways of connecting to ElasticSearch
from within a Hadoop task.

* By default, each map task will spin up a
  [transport client](http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html)
  which will attempt to connect to some ElasticSearch webnode.  By
  default it will look for this webnode on the same machine as the
  task itself is running on.  This is convenient in the common case
  when each Hadoop tasktracker is also an ElasticSearch webnode
  (datanodes may, of course, live elsewhere).

* Each map task can also be configured to spin up its own
  [embedded ElasticSearch node](http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/client.html#node-client)
  which directly connects to an ElasticSearch cluster.

The following options control this behavior:

* `--es_transport` -- Use a transport client instead of an embedded node. True by default.
* `--es_transport_host` -- When using a transport client, the host of
  the ElasticSearch webnode to connect to.  Defaults to `localhost`.
* `--es_transport_port` -- When using a transport client, the port of
  the ElasticSearch webnode to connect to.  Defaults to `9300`.

### Writing data to ElasticSearch

Here's an example which would write all its output data to the index
`twitter` in the type `tweet`:

```
$ wu hadoop my_job.rb --input=/some/hdfs/input/path --output=es://twitter/tweet
```

#### Data Format & Routing

It's always assumed that the output of the reducer is
newline-delimited, JSON formatted data.  Most fields in the each
record are passed through unmodified or read.  But some fields are
important:

* `_id` - if this field is present then it will be used as the
  document ID of the record created in ElasticSearch.  This is the
  right way to ensure that a write updates an existing document
  instead of creating a new document.  The name of this field (`_id`)
  can be modified with the `--es_id_field` option.

* `_mapping` - if this field is present then it will be used as the
  type the document is written to, no matter what was passed on the
  command-line as the `--output`.  This is the right way to allow
  writing to multiple types depending on the document.  The name of
  this field (`_mapping`) can be modified with the
  `--es_mapping_field` option.  And, yes, this field probably should
  have been called `_type`...

* `_index` - if this field is present then it will be used as the
  index the document is written to, no matter what was passed on the
  command-line as the `--output`.  This is the right way to allow
  writing to multiple types depending on the document.  The name of
  this field (`_index`) can be modified with the `--es_index_field`
  option.

#### Optimization

It's not unusual to prepare an ElasticSearch index for bulk writing
before executing a Hadoop job to write to it.  The following
operations should be enabled for best performance:

* turn `index.number_of_replicas` down to 0 to ensure that there are
  as few shards (copies) of the data as possible that need to be
  updated on each write
  
* turn `index.refresh_interval` to -1 to ensure that ElasticSearch
  doesn't allocate any of its resources refreshing data for search
  instead of indexing.

It's also a good idea to have created all mappings up front, before
loading.

Wonderdog provides the `--es_bulk_size` option which sets the size of
batch writes sent to ElasticSearch (default: 1000).  Increasing this
number can be appropriate and lead to higher throughput in some
situations.

### Reading data from ElasticSearch

Here's an example which would read all its input data from the index
`twitter` in the type `tweet`:

```
$ wu hadoop my_job.rb --input=es://twitter/tweet --output=/some/hdfs/output/path
```

This would read in every single `tweet` record.  This can be
customized using the full power of ElasticSearch by providing an
arbitrary ElasticSearch JSON query via the `--es_query` option.
Wonderdog will run the query at Hadoop job submission time and use the
result-set as the input data.

The result-set will be presented to Hadoop as newline-delimited,
JSON-formatted data.

Here's an example, which would capture only tweets about Chicago:

```
$ wu hadoop my_job.rb --input=es://twitter/tweet --output=/some/hdfs/output/path --es_query='{"query": {"match":{"text": "Chicago"}}}'
```

#### Optimization

Wonderdog uses ElasticSearch's
[scroll API](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-scroll.html)
to fetch data from ElasticSearch. There are several options which can
be used to tune the usage of this API for better performance:

* `--es_input_splits` -- the number of input splits to create
* `--es_request_size` -- the number of documents to request at a time (defaults to 50)
* `--es_scroll_timeout` -- the amount of time to wait on each scroll / longest running map task (defaults to 5 minutes)

The larger the dataset and the fewer the input splits, the larger the
data that needs to be processed (and hence scrolled through) within
each task, the longer the scroll timeout should be set for.
Essentially, no task should take longer to complete than the scroll
timeout.

It's recommended to read data out of ElasticSearch into a temporary
copy in HDFS which can then be used for more intensive processing.

<a name="pig">
# Pig

The most up-to-date (and simplest) way to store data into
elasticsearch with hadoop is to use the Pig Store Function. You can
write both delimited and json data to elasticsearch as well as read
data from elasticsearch.

## Storing Data

### Storing tabular data

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

### Storing json data

You can store json data just as easily.

```pig
ufo_sightings = LOAD '/data/domestic/aliens/ufo_awesome.tsv.json' AS (json_record:chararray);
STORE ufo_sightings INTO 'es://$INDEX/$OBJ?json=true&size=1000' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage();
```

## Loading Data

Easy too.

```pig
-- dump some of the ufo sightings index based on free text query
alien_sightings = LOAD 'es://ufo_sightings/ufo_sightings?q=alien' USING com.infochimps.elasticsearch.pig.ElasticSearchStorage() AS (doc_id:chararray, contents:chararray);
DUMP alien_sightings;
```

## ElasticSearchStorage Constructor

The constructor to the UDF can take two arguments (in the following order):

* ```esConfig``` - The full path to where elasticsearch.yml lives on the machine launching the hadoop job
* ```esPlugins``` - The full path to where the elasticsearch plugins directory lives on the machine launching the hadoop job

### Query Parameters

There are a few query paramaters available:

* ```json``` - (STORE only) When 'true' indicates to the StoreFunc that pre-rendered json records are being indexed. Default is false.
* ```size``` - When storing, this is used as the bulk request size (the number of records to stack up before indexing to elasticsearch). When loading, this is the number of records to fetch per request. Default 1000.
* ```q``` - (LOAD only) A free text query determining which records to load. If empty, matches all documents in the index.
* ```id``` - (STORE only) The name of the field to use as a document id. If blank (or -1) the documents are assumed to have no id and are assigned one by elasticsearch.
* ```tasks``` - (LOAD only) The number of map tasks to launch. Default 100.

Note that elasticsearch.yml and the plugins directory are distributed to every machine in the cluster automatically via hadoop's distributed cache mechanism.

<a name="utilities">
# Command-Line Utilities

There are a number of convenience commands in ```estool```. Most of
the common REST API operations have be mapped. Enumerating a few:

* Print status of all indices as a json hash to the terminal

```
$ estool -c <elasticsearch_host> status
```

* Check cluster health (red,green,yellow,relocated shards, etc)

```
$ estool -c <elasticsearch_host>  health
```

* Set replicas for an index

```
$ estool set_replication -c <elasticsearch_host> --index <index_name> --replicas <num_replicas>
```

* Optimize an index

```
$ estool optimize -c <elasticsearch_host> --index <index_name>
```

* Snapshot an index

```
$ estool snapshot -c <elasticsearch_host> --index <index_name>
```

* Delete an index

```
$ estool delete -c <elasticsearch_host> --index <index_name>
```
