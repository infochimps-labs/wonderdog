package com.infochimps.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.OutputCommitter;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.ExceptionsHelper;

/**
   
   Output format for writing hashmaps into elasticsearch. Records are batched up and sent
   in a one-hop manner to the elastic search data nodes that will index them.
   
 */
public class ElasticSearchOutputFormat extends OutputFormat<NullWritable, MapWritable> implements Configurable {
    
    static Log LOG = LogFactory.getLog(ElasticSearchOutputFormat.class);
    private Configuration conf = null;

    protected class ElasticSearchRecordWriter extends RecordWriter<NullWritable, MapWritable> {

        private Node node;
        private Client client;
        private String indexName;
        private int bulkSize;
        private int idField;
        private String objType;
        private String[] fieldNames;
        
        // Used for bookkeeping purposes
        private AtomicLong totalBulkTime  = new AtomicLong();
        private AtomicLong totalBulkItems = new AtomicLong();
        private Random     randgen        = new Random();        
        private long       runStartTime   = System.currentTimeMillis();
        
        private volatile BulkRequestBuilder currentRequest;
        
        public ElasticSearchRecordWriter(TaskAttemptContext context) {
            Configuration conf = context.getConfiguration();
            this.indexName  = conf.get("elasticsearch.index.name");
            this.bulkSize   = Integer.parseInt(conf.get("elasticsearch.bulk.size"));
            LOG.info("field names: "+conf.get("elasticsearch.field.names"));
            this.fieldNames = conf.get("elasticsearch.field.names").split(",");
            this.idField    = Integer.parseInt(conf.get("elasticsearch.id.field"));
            LOG.info("key field name: "+fieldNames[idField]);
            this.objType    = conf.get("elasticsearch.object.type");
            System.setProperty("es.config",conf.get("elasticsearch.config"));
            System.setProperty("es.path.plugins",conf.get("elasticsearch.plugins.dir"));
            start_embedded_client();
            initialize_index(indexName);
            currentRequest = client.prepareBulk();
        }

        /**
           Need to index any remaining content.
         */
        public void close(TaskAttemptContext context) throws IOException {
            if (currentRequest.numberOfActions() > 0) {            
                try {
                    BulkResponse response = currentRequest.execute().actionGet();
                } catch (Exception e) {
                    LOG.warn("Bulk request failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            LOG.info("Closing record writer");
            client.close();
            // LOG.info("Client is closed")
            // if (node != null) {
            //     node.close();
            // }
            LOG.info("Record writer closed.");
        }

        public void write(NullWritable key, MapWritable fields) throws IOException {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (Map.Entry<Writable, Writable> entry : fields.entrySet()) {
                String name  = entry.getKey().toString();
                String value = entry.getValue().toString();
                builder.field(name, value);
            }
            builder.endObject();
            if (idField == -1) {
                // Document has no inherent id
                currentRequest.add(Requests.indexRequest(indexName).type(objType).source(builder));
            } else {
                try {
                    Text mapKey = new Text(fieldNames[idField]);
                    String record_id = fields.get(mapKey).toString();
                    currentRequest.add(Requests.indexRequest(indexName).id(record_id).type(objType).create(false).source(builder));
                } catch (Exception e) {
                    LOG.info("Increment bad record counter");
                }
            }
            processBulkIfNeeded();
        }

        private void processBulkIfNeeded() {
            totalBulkItems.incrementAndGet();
            if (currentRequest.numberOfActions() >= bulkSize) {
                try {
                    long startTime        = System.currentTimeMillis();
                    BulkResponse response = currentRequest.execute().actionGet();
                    totalBulkTime.addAndGet(System.currentTimeMillis() - startTime);
                    if (randgen.nextDouble() < 0.1) {
                        LOG.info("Indexed [" + totalBulkItems.get() + "] in [" + (totalBulkTime.get()/1000) + "s] of indexing"+"[" + ((System.currentTimeMillis() - runStartTime)/1000) + "s] of wall clock"+" for ["+ (float)(1000.0*totalBulkItems.get())/(System.currentTimeMillis() - runStartTime) + "rec/s]");
                    }
                } catch (Exception e) {
                    LOG.warn("Bulk request failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
                currentRequest = client.prepareBulk();
            }
        }

        private void initialize_index(String indexName) {
            LOG.info("Initializing index");
            try {
                client.admin().indices().prepareCreate(indexName).execute().actionGet();
            } catch (Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                    LOG.warn("Index ["+indexName+"] already exists");
                }
            }
        }

        //
        // Starts an embedded elasticsearch client (ie. data = false)
        //
        private void start_embedded_client() {
            LOG.info("Starting embedded elasticsearch client ...");
            this.node   = NodeBuilder.nodeBuilder().client(true).node();
            this.client = node.client();
        }
    }

    public RecordWriter<NullWritable, MapWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchRecordWriter(context);
    }

    public void setConf(Configuration conf) {
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // TODO Check if the object exists?
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchOutputCommitter();
    }
}
