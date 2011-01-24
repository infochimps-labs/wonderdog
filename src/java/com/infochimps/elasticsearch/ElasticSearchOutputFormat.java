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
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

public class ElasticSearchOutputFormat extends OutputFormat<NullWritable, MapWritable> implements Configurable{
    static Log LOG = LogFactory.getLog(ElasticSearchOutputFormat.class);
    private Configuration conf = null;

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

    protected class ElasticSearchRecordWriter extends RecordWriter<NullWritable, MapWritable> {

        private Node node;
        private Client client;
        private volatile BulkRequestBuilder currentRequest;
        
        public ElasticSearchRecordWriter(Node node, Client client) {
            this.node   = node;
            this.client = client;
            currentRequest = client.prepareBulk();
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            client.close();
            if (node != null) {
                node.close();
            }
        }

        @Override
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
                // Use one of the docuement's fields as the id
                // currentRequest.add(Requests.indexRequest(indexName).type(objType).create(false).source(builder));
            }
            processBulkIfNeeded();
        }

        private void processBulkIfNeeded() {
            totalBulkItems.incrementAndGet();
            if (currentRequest.numberOfActions() >= bulkSize) {
                LOG.info("Sending bulk request of ["+currentRequest.numberOfActions()+"] records");
                try {
                    BulkResponse response = currentRequest.execute().actionGet();
                } catch (Exception e) {
                    LOG.warn("Bulk request failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
                currentRequest = client.prepareBulk();
            }
        }
    }

    @Override
        public RecordWriter<NullWritable, MapWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchRecordWriter(node, client);
    }

    @Override
    public void setConf(Configuration conf) {
        this.indexName  = conf.get("wonderdog.index.name");
        this.bulkSize   = Integer.parseInt(conf.get("wonderdog.bulk.size"));
        this.fieldNames = conf.get("wonderdog.field.names").split(",");
        this.idField    = Integer.parseInt(conf.get("wonderdog.id.field"));
        this.objType    = conf.get("wonderdog.object.type");
        System.setProperty("es.path.plugins",conf.get("wonderdog.plugins.dir"));
        System.setProperty("es.config",conf.get("wonderdog.config"));

        // Basic setup
        start_embedded_client();
        initialize_index(indexName);        
    }

    private void initialize_index(String indexName) {
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


    public Configuration getConf() {
        return conf;
    }
  
    public void checkOutputSpecs(JobContext context) throws IOException,
        InterruptedException {
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchOutputCommitter();
    }
}
