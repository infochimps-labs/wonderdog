package com.infochimps.elasticsearch.wonderdog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

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


//
// Simple one-hop bulk indexing hadoop job for elasticsearch. It accepts
// tsv documents, creates batch index requests, and sends records directly
// to the elasticsearch data node that's going to actually index them.
//
public class WonderDog extends Configured implements Tool {

    private final static Log LOG = LogFactory.getLog(WonderDog.class);

    // enum for setting up atomic counters
    enum BulkRequests {
        SUCCEEDED,
        FAILED,
        INDEXED
    }

    //
    // Naive tsv indexer.
    //
    public static class IndexMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
      
        private Node node;
        private Client client;
        private String indexName;
        private int bulkSize;
        private int idField;
        private String objType;
        private String[] fieldNames;
        private volatile BulkRequestBuilder currentRequest;

        // Used for bookkeeping purposes
        private AtomicLong totalBulkTime  = new AtomicLong();
        private AtomicLong totalBulkItems = new AtomicLong();
        private Random     randgen        = new Random();
        private long       runStartTime   = System.currentTimeMillis();

        // Used for hadoop counters
        private Counter succeededRequests;
        private Counter failedRequests;
        private Counter indexedRecords;

        // Used to hold records and dump in case of failure
        private ArrayList<String> records;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            // Add to arraylist
            records.add(value.toString());
            //
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for(int i = 0; i < fields.length; i++) {
                if (i < fieldNames.length) {
                    builder.field(fieldNames[i], fields[i]);
                }
            }
            builder.endObject();
            if (idField == -1) {
                currentRequest.add(Requests.indexRequest(indexName).type(objType).source(builder));
            } else {
                currentRequest.add(Requests.indexRequest(indexName).type(objType).id(fields[idField]).create(false).source(builder));
            }
            processBulkIfNeeded(context);
        }

        private void processBulkIfNeeded(Context context) {
            totalBulkItems.incrementAndGet();
            if (currentRequest.numberOfActions() >= bulkSize) {
                LOG.info("Sending bulk request of ["+currentRequest.numberOfActions()+"] records");
                try {
                    long startTime        = System.currentTimeMillis();
                    BulkResponse response = currentRequest.execute().actionGet();
                    totalBulkTime.addAndGet(System.currentTimeMillis() - startTime);
                    if (randgen.nextDouble() < 0.1) {
                        LOG.info("Indexed [" + totalBulkItems.get() + "] in [" + (totalBulkTime.get()/1000) + "s] of indexing"+"[" + ((System.currentTimeMillis() - runStartTime)/1000) + "s] of wall clock"+" for ["+ (float)(1000.0*totalBulkItems.get())/(System.currentTimeMillis() - runStartTime) + "rec/s]");
                    }
                    if (response.hasFailures()) {
                        failedRequests.increment(1);
                        // write failed records out to hdfs for reloading
                        for(String record : records) {
                            context.write(NullWritable.get(), new Text(record));
                        }
                    } else {
                        succeededRequests.increment(1);
                        indexedRecords.increment(bulkSize);
                    }
                } catch (Exception e) {
                    LOG.warn("Bulk request failed: " + e.getMessage());
                    failedRequests.increment(1);
                    throw new RuntimeException(e);
                }
                records.clear();
                currentRequest = client.prepareBulk();
            }
        }

        //
        // Called once at the beginning of the map task. Sets up the indexing job.
        //
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            // Set all task level config
            Configuration conf = context.getConfiguration();
            this.indexName  = conf.get("wonderdog.index.name");
            this.bulkSize   = Integer.parseInt(conf.get("wonderdog.bulk.size"));
            this.fieldNames = conf.get("wonderdog.field.names").split(",");
            this.idField    = Integer.parseInt(conf.get("wonderdog.id.field"));
            this.objType    = conf.get("wonderdog.object.type");
            System.setProperty("es.path.plugins",conf.get("wonderdog.plugins.dir"));
            System.setProperty("es.config",conf.get("wonderdog.config"));

            //
            // Initialize arraylist for storing string records. Used for sending
            // failed records to disk for later re-indexing.
            //
            this.records = new ArrayList<String>(bulkSize);

            // Counters
            succeededRequests = context.getCounter(BulkRequests.SUCCEEDED);
            failedRequests    = context.getCounter(BulkRequests.FAILED);
            indexedRecords    = context.getCounter(BulkRequests.INDEXED);

            // Basic setup
            start_embedded_client();
            initialize_index(indexName);
            currentRequest = client.prepareBulk();
        } 

        //
        // Called once at the end of map task.
        //
        public void cleanup() {
            LOG.info("Closing embedded elasticsearch client...");
            client.close();
            if (node != null) {
                node.close();
            }
            LOG.info("Indexed [" + totalBulkItems.get() + "] in [" + totalBulkTime.get() + "ms]");
        }

        //
        // Initialize the index, catch the possibility of it already existing.
        //
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

    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(WonderDog.class);
        job.setJobName("WonderDog");
        job.setMapperClass(IndexMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            System.out.println(args[i]);
            other_args.add(args[i]);
        }
        // Here we need _both_ an input path and an output path.
        // Output stores failed records so they can be re-indexed
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
    
        try {
            job.waitForCompletion(true);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WonderDog(), args);
        System.exit(res);
    }
}
