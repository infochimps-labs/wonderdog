package com.infochimps.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import org.elasticsearch.index.query.xcontent.QueryBuilders;


public class ElasticSearchInputFormat extends InputFormat<Text, Text> implements Configurable {

    static Log LOG = LogFactory.getLog(ElasticSearchInputFormat.class);
    private Configuration conf = null;

    private Node node;
    private Client client;

    private Integer scrollSize;
    private String scrollId;
    private Long numHits;
    private Long numSplits;
    private Long numSplitRecords;
    private String indexName;
    private String objType;
    
    private static final String ES_SCROLL_SIZE = "elasticsearch.scroll.size";
    private static final String ES_NUMHITS = "elasticsearch.num.hits";
    private static final String ES_NUM_SPLITS = "elasticsearch.num.input.splits";
    private static final String ES_SCROLL_ID = "elasticsearch.scroll.id";
    private static final String ES_CONFIG_NAME = "elasticsearch.yml";
    private static final String ES_PLUGINS_NAME = "plugins";
    private static final String ES_INDEX_NAME = "elasticsearch.index.name";
    private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
    private static final String ES_CONFIG = "es.config";
    private static final String ES_PLUGINS = "es.path.plugins";

    private static final String SLASH = "/";
    
    public RecordReader<Text,Text> createRecordReader(InputSplit inputSplit,
                                                              TaskAttemptContext context) {
        return new ElasticSearchRecordReader();
    }

    /**
       Sort of silly, really. All we're going to do is use the number of desired splits
       and break the total number of results into that many splits.

       FIXME: Need to check that the number of splits isn't larger than the number of records
     */
    public List<InputSplit> getSplits(JobContext context) {
        LOG.info("Generating approximately ["+numSplits+"] splits for ["+numHits+"] results using elasticsearch scroll id ["+scrollId+"]");

        Configuration conf = context.getConfiguration();
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits.intValue());
        for(int i = 0; i < numSplits+1; i++) {
            splits.add(new ElasticSearchSplit());
        }
        return splits;
    }

    /**
       This is where we will get information from the outside world such as the cluster to talk to,
       which index and object type to refer to, as well as more mundane things like the number of
       documents per scroll request to use. Need to create a single connection object here and
       determine things like the scroll id, etc.
     */
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        this.indexName = conf.get(ES_INDEX_NAME);
        this.objType = conf.get(ES_OBJECT_TYPE);
        this.scrollSize = Integer.parseInt(conf.get(ES_SCROLL_SIZE));
        this.numSplits = Long.parseLong(conf.get(ES_NUM_SPLITS));
        
        // FIXME: need to get this in some other way
        System.setProperty(ES_CONFIG, "/etc/elasticsearch/elasticsearch.yml");
        System.setProperty(ES_PLUGINS, "/usr/local/share/elasticsearch/plugins");
        
        start_embedded_client();
        initiate_scan();
    }

    /**
       Doesn't have to do anything other than make the configuration publicly accessible.
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    //
    // Starts an embedded elasticsearch client (ie. data = false)
    //
    private void start_embedded_client() {
        LOG.info("Starting embedded elasticsearch client ...");
        this.node   = NodeBuilder.nodeBuilder().client(true).node();
        this.client = node.client();
    }

    /**
       FIXME: We would sure like to specify more specific kinds of queries other than
       'match all'
     */
    private void initiate_scan() {
        SearchResponse response = client.prepareSearch(indexName)
            .setTypes(objType)
            .setSearchType(SearchType.SCAN)
            .setScroll("10m") // this should be settable
            .setQuery(QueryBuilders.matchAllQuery())
            .setFrom(0)
            .setSize(scrollSize)
            .execute()
            .actionGet();
        this.numHits = response.hits().totalHits();
        this.scrollId = response.scrollId();
        this.numSplitRecords = (numHits/numSplits);        
    }
    
    protected class ElasticSearchRecordReader extends RecordReader<Text, Text> {

        private Text currentKey;
        private Text currentValue;
        private boolean val = true;
        public ElasticSearchRecordReader() {
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if(val) {
                val = false;
                return true;
            }
            return false;
        }
    
        @Override
        public Text getCurrentKey() {
            return new Text("howdy");
        }
    
        @Override
        public Text getCurrentValue() {
            return new Text("dood");
        }
        
        @Override
        public float getProgress() throws IOException {
            return 0;
        }
    }
}
