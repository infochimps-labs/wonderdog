import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.elasticsearch.node.Node;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.client.transport.TransportClient;

public class ElasticBulkLoader extends Configured implements Tool {
  public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
    private JobConf jobconf;
    private Node node;
    private Client client;
    private volatile BulkRequestBuilder currentRequest;
    private int bulkSize;

    private AtomicLong totalBulkTime  = new AtomicLong();
    private AtomicLong totalBulkItems = new AtomicLong();
    private Random     randgen        = new Random();
    private long       runStartTime   = System.currentTimeMillis();
        
    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      add_tweet_to_bulk(value);
      if (randgen.nextDouble() < 0.001) { output.collect(key, value); }
    }

    public void add_tweet_to_bulk(Text value) {
      String[] fields = value.toString().split("\t"); // Raw fields of tweet object
      try {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("tweet_id",                fields[0]);
        builder.field("created_at",              fields[1]);
        builder.field("user_id",                 fields[2]);
        builder.field("screen_name",             fields[3]);
        builder.field("search_id",               fields[4]);
        builder.field("in_reply_to_user_id",     fields[5]);
        builder.field("in_reply_to_screen_name", fields[6]);
        builder.field("in_reply_to_search_id",   fields[7]);
        builder.field("in_reply_to_status_id",   fields[8]);
        builder.field("text",                    fields[9]);
        // builder.field("source",               fields[10]);
        // builder.field("lang",                 fields[11]);
        // builder.field("lat",                  fields[12]);
        // builder.field("lng",                  fields[13]);
                
        builder.endObject();
        currentRequest.add(Requests.indexRequest("foo").type("tweet").id(fields[0]).create(true).source(builder));
        processBulkIfNeeded();
      } catch (Exception e) {
        System.out.println("There was some sort of problem here in trying to create a new index request");
      }
    }

    private void processBulkIfNeeded() {
      totalBulkItems.incrementAndGet();
      if (currentRequest.numberOfActions() >= bulkSize) {
        try {
          long startTime = System.currentTimeMillis();
          BulkResponse response = currentRequest.execute().actionGet();
          totalBulkTime.addAndGet(System.currentTimeMillis() - startTime);
          if (randgen.nextDouble() < 0.005) {
            System.out.println("Indexed [" + totalBulkItems.get() + "] in [" + (totalBulkTime.get()/1000) + "s] of indexing"+"[" + ((System.currentTimeMillis() - runStartTime)/1000) + "s] of wall clock"+" for ["+ (float)(1000.0*totalBulkItems.get())/(System.currentTimeMillis() - runStartTime) + "rec/s]");
          }
          if (response.hasFailures()) {
            System.out.println("failed to execute" + response.buildFailureMessage());
          }
        } catch (Exception e) {
          System.out.println("Bulk request failed: " + e.getMessage());
          throw new RuntimeException(e);
        }
        // Create the next (empty) bulk request
        currentRequest = client.prepareBulk();
      }
    }

    //
    // This happens at the very beginning of the map task
    //
    public void configure(JobConf job) {
      this.jobconf = job;

      bulkSize = 5;
      System.out.println("bulk size set to "+bulkSize);
      System.setProperty("es.path.plugins","/usr/lib/elasticsearch/plugins");
      System.setProperty("es.config","/etc/elasticsearch/elasticsearch.yml");
      
      // node = NodeBuilder.nodeBuilder().client(true).data(false).node();
      // client = node.client();
      //
      client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("10.195.10.207", 9300));
      client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("10.195.10.207", 9301));
      client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("10.195.10.207", 9302));
      client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("10.195.10.207", 9303));

      try {
        client.admin().indices().prepareCreate("foo").execute().actionGet();
      } catch (Exception e) {
        if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
          System.out.println("Index foo already exists");
        }
      }
      // Start the first, empty bulk request
      currentRequest = client.prepareBulk();
    }

    //
    // This happens at the very end of the map task
    //
    public void close() {
      // client.admin().indices().prepareRefresh("foo").execute().actionGet();
      client.close();
      // node.close();
       System.out.println("Indexed [" + totalBulkItems.get() + "] in [" + totalBulkTime.get() + "ms]");
    }
  }

  // public static void runJob(String[] args)
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), ElasticBulkLoader.class);

    GenericOptionsParser parser = new GenericOptionsParser(conf,args);
    conf.setJobName("ElasticBulkLoader");
    conf.setNumReduceTasks(0);
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setMapperClass(Map.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    List<String> other_args = new ArrayList<String>();
    for (int i=0; i < args.length; ++i) {
      System.out.println(args[i]);
      other_args.add(args[i]);
    }

    FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    try {
      JobClient.runJob(conf);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ElasticBulkLoader(), args);
    System.exit(res);
  }
}
