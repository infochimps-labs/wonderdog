package com.infochimps.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.*;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.ExceptionsHelper;

import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParseException;

/**
   
   Hadoop OutputFormat for writing arbitrary MapWritables (essentially HashMaps) into Elasticsearch. Records are batched up and sent
   in a one-hop manner to the elastic search data nodes that will index them.
   
*/
public class ElasticSearchStreamingOutputFormat<K, V> implements Configurable, OutputFormat<K, V> {
    
    static Log LOG = LogFactory.getLog(ElasticSearchStreamingOutputFormat.class);
    private Configuration conf = null;

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        return (RecordWriter) new ElasticSearchStreamingRecordWriter(job);
    }

    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        // TODO Check if the object exists?
    }
    
    protected class ElasticSearchStreamingRecordWriter<K, V> implements RecordWriter<K, V> {

        // Used for bookkeeping purposes
        private AtomicLong totalBulkTime  = new AtomicLong();
        private AtomicLong totalBulkItems = new AtomicLong();
        private Random     randgen        = new Random();
        private long       runStartTime   = System.currentTimeMillis();

        // Job settings we need to control directly from Java options.
        private static final String ES_INDEX_OPT    = "elasticsearch.index.name";
	private static final String ES_INDEX        = "hadoop";
	private              String indexName;
	
        private static final String ES_TYPE_OPT     = "elasticsearch.type.name";
	private static final String ES_TYPE         = "streaming_record";
	private              String typeName;
	
        private static final String ES_ID_FIELD_OPT = "elasticsearch.id.field";
	private static final String ES_ID_FIELD     = "_id";
	private              String idFieldName;
	
        private static final String ES_BULK_SIZE_OPT     = "elasticsearch.bulk.size";
	private static final String ES_BULK_SIZE         = "100";
	private              int    bulkSize;

	// Elasticsearch internal settings required to make a client
	// connection.
        private static final String ES_CONFIG_OPT        = "es.config";
        private static final String ES_CONFIG            = "/etc/elasticsearch/elasticsearch.yml";
        private static final String ES_PLUGINS_OPT       = "es.path.plugins";
	private static final String ES_PLUGINS           = "/usr/local/share/elasticsearch/plugins";
        private              Node               node;
        private              Client             client;
        private volatile     BulkRequestBuilder currentRequest;
	private              ObjectMapper       mapper;

        /**
           Instantiates a new RecordWriter for Elasticsearch
           <p>
           The following properties control how records will be written:
           <ul>
           <li><b>elasticsearch.index.name</b> - The name of the Elasticsearch index data will be written to. It does not have to exist ahead of time.  (default: "hadoop")</li>
	   <li><b>elasticsearch.type.name</b> - The name of the Elasticsearch type for objects being indexed.  It does not have to exist ahead of time.  (default: "streaming_record")</li>
           <li><b>elasticsearch.bulk.size</b> - The number of records to be accumulated into a bulk request before writing to elasticsearch.  (default: 1000)</li>
           <li><b>elasticsearch.id.field</b> - The the name of a field in the input JSON record that contains the document's id.</li>
           </ul>
           Elasticsearch properties like "es.config" and "es.path.plugins" are still relevant and need to be set.
	   </p>
	*/
        public ElasticSearchStreamingRecordWriter(JobConf conf) {
            this.indexName     = conf.get(ES_INDEX_OPT,    ES_INDEX);
	    this.typeName      = conf.get(ES_TYPE_OPT,     ES_TYPE);
	    this.idFieldName   = conf.get(ES_ID_FIELD_OPT, ES_ID_FIELD);
	    this.bulkSize      = Integer.parseInt(conf.get(ES_BULK_SIZE_OPT, ES_BULK_SIZE));
	    
	    LOG.info("Writing "+Integer.toString(bulkSize)+" records per batch to /"+indexName+"/"+typeName+" using ID field '"+idFieldName+"'");

	    String esConfigPath  = conf.get(ES_CONFIG_OPT,  ES_CONFIG);
	    String esPluginsPath = conf.get(ES_PLUGINS_OPT, ES_PLUGINS);
	    System.setProperty(ES_CONFIG_OPT,esConfigPath);
	    System.setProperty(ES_PLUGINS_OPT,esPluginsPath);
	    LOG.info("Using Elasticsearch configuration file at "+esConfigPath+" and plugin directory "+esPluginsPath);
            
            startEmbeddedClient();
            initializeIndex();
            this.currentRequest = client.prepareBulk();
	    this.mapper = new ObjectMapper();
        }

	/**
	   Start an embedded Elasticsearch client.  The client will
	   not be a data node and will not store data locally.  The
	   client will connect to the target Elasticsearch cluster as
	   a client node, enabling one-hop writes for all data.
	*/
        private void startEmbeddedClient() {
            LOG.info("Starting embedded Elasticsearch client (non-datanode)...");
            this.node   = NodeBuilder.nodeBuilder().client(true).node();
            this.client = node.client();
	    LOG.info("Successfully joined Elasticsearch cluster '"+ClusterName.clusterNameFromSettings(node.settings())+'"');
        }

	/**
	   Create the index we will write to if necessary.
	*/
        private void initializeIndex() {
            LOG.info("Initializing index /"+indexName);
            try {
                client.admin().indices().prepareCreate(indexName).execute().actionGet();
		LOG.info("Created index /"+indexName);
            } catch (Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                    LOG.info("Index /"+indexName+" already exists");
                } else {
		    LOG.error("Could not initialize index /"+indexName, e);
		}
            }
        }

        /**
           Writes a single input JSON record.  If the record contains
           a field "elasticsearch.id.field" then that the value of
           that field will be used to update an existing record in
           Elasticsearch.  If not such field exists, the record will
           be created and assigned an ID.
	*/
        public void write(K key, V value) throws IOException {
	    String json = ((Text) key).toString();
	    try {
		Map<String, Object> data = mapper.readValue(json, Map.class);
		if (data.containsKey(idFieldName)) {
		    Object idValue = data.get(idFieldName);
		    if ((idValue instanceof String)) {
			currentRequest.add(Requests.indexRequest(indexName).id((String) idValue).type(typeName).create(false).source(json));
		    }
		} else {
		    currentRequest.add(Requests.indexRequest(indexName).type(typeName).source(json));
		}
		sendRequestIfNeeded();
	    } catch(Exception e) {
		if (ExceptionsHelper.unwrapCause(e) instanceof JsonParseException) {
		    LOG.debug("Bad record: "+json);
		    return;
		} else {
		    LOG.error("Could not write record: "+json, e);
		}
	    }
	}

        /**
           Close the Elasticsearch client, sending out one last bulk
           write if necessary.
	*/
        public void close(Reporter reporter) throws IOException {
	    sendRequestIfMoreThan(0);
	    LOG.info("Shutting down Elasticsearch client...");
            if (client != null) client.close();
            if (node   != null) node.close();
            LOG.info("Successfully shut down Elasticsearch client");
        }

        /**
           Indexes content to elasticsearch when <b>elasticsearch.bulk.size</b> records have been accumulated.
	*/
        private void sendRequestIfNeeded() {
	    sendRequestIfMoreThan(bulkSize);
	}

	private void sendRequestIfMoreThan(int size) {
            totalBulkItems.incrementAndGet();
            if (currentRequest.numberOfActions() > size) {
		long startTime        = System.currentTimeMillis();
		BulkResponse response = currentRequest.execute().actionGet();
		totalBulkTime.addAndGet(System.currentTimeMillis() - startTime);
		if (randgen.nextDouble() < 0.1) {
		    LOG.info("Indexed [" + totalBulkItems.get() + "] in [" + (totalBulkTime.get()/1000) + "s] of indexing"+"[" + ((System.currentTimeMillis() - runStartTime)/1000) + "s] of wall clock"+" for ["+ (float)(1000.0*totalBulkItems.get())/(System.currentTimeMillis() - runStartTime) + "rec/s]");
		}
                currentRequest = client.prepareBulk();
            }
        }
    }

    public void setConf(Configuration conf) {
    }

    public Configuration getConf() {
        return conf;
    }

    public ElasticSearchStreamingOutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchStreamingOutputCommitter();
    }
}
