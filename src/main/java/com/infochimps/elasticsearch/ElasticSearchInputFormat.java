package com.infochimps.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

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
import org.elasticsearch.search.SearchHit;
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
        return new ElasticSearchRecordReader(numSplitRecords, scrollSize, scrollId);
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

        //
        // Need to ensure that this is set in the hadoop configuration so we can
        // instantiate a local client. The reason is that no files are in the
        // distributed cache when this is called.
        //
        System.setProperty(ES_CONFIG, conf.get(ES_CONFIG));
        System.setProperty(ES_PLUGINS, conf.get(ES_PLUGINS));
        
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

        private Node node;
        private Client client;
    
        private String indexName;
        private String objType;
        private Long numSplitRecords;
        private Integer scrollSize;
        private String scrollId;
        private Text currentKey;
        private Text currentValue;
        private Integer recordsRead;
        private Iterator<SearchHit> hitsItr = null;        
        
        public ElasticSearchRecordReader(Long numSplitRecords, Integer scrollSize, String scrollId) {
            this.numSplitRecords = numSplitRecords;
            this.scrollSize = scrollSize;
            this.scrollId = scrollId;
            System.out.println(scrollId);
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            this.indexName = conf.get(ES_INDEX_NAME);
            this.objType    = conf.get(ES_OBJECT_TYPE);
            LOG.info("Initializing elasticsearch record reader on index ["+indexName+"] and object type ["+objType+"]");

            //
            // Fetches elasticsearch.yml and the plugins directory from the distributed cache
            //
            try {
                String taskConfigPath = HadoopUtils.fetchFileFromCache(ES_CONFIG_NAME, conf);
                LOG.info("Using ["+taskConfigPath+"] as es.config");
                String taskPluginsPath = HadoopUtils.fetchArchiveFromCache(ES_PLUGINS_NAME, conf);
                LOG.info("Using ["+taskPluginsPath+"] as es.plugins.dir");
                System.setProperty(ES_CONFIG, taskConfigPath);
                System.setProperty(ES_PLUGINS, taskPluginsPath+SLASH+ES_PLUGINS_NAME);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            start_embedded_client();
            recordsRead = 0;
        }

        //
        // Starts an embedded elasticsearch client (ie. data = false)
        //
        private void start_embedded_client() {
            LOG.info("Starting embedded elasticsearch client ...");
            this.node   = NodeBuilder.nodeBuilder().client(true).node();
            this.client = node.client();
        }

        private Iterator<SearchHit> fetchNextHits() {
            SearchResponse response = client.prepareSearchScroll(scrollId)
                .setScroll("10m")
                .execute()
                .actionGet();
            return response.hits().iterator();
        }
        
        /**
           Maintains a buffer of records from elasticsearch. If the buffer
           is empty _and_ the number of records read by _this_ record reader
           hasn't exceeded the numSplitRecords then a new scroll get request
           is made to elasticsearch.
         */
        @Override
        public boolean nextKeyValue() throws IOException {
            if (hitsItr!=null) {
                if (recordsRead <= numSplitRecords && hitsItr.hasNext()) {
                    SearchHit hit = hitsItr.next();
                    currentKey = new Text(hit.id());
                    currentValue = new Text(hit.sourceAsString());
                    recordsRead += 1;
                } else {
                    hitsItr = null;
                }
                return true;
            } else {
                if (recordsRead <= numSplitRecords) {
                    hitsItr = fetchNextHits();
                    if (hitsItr.hasNext()) {
                        SearchHit hit = hitsItr.next();
                        currentKey = new Text(hit.id());
                        currentValue = new Text(hit.sourceAsString());
                        recordsRead += 1;
                        return true;
                    }
                    return false;
                }
                return false;
            }
        }
    
        @Override
        public Text getCurrentKey() {
            return currentKey;
        }
    
        @Override
        public Text getCurrentValue() {
            return currentValue;
        }
        
        @Override
        public float getProgress() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            LOG.info("Closing record reader");
            client.close();
            LOG.info("Client is closed");
            if (node != null) {
                 node.close();
            }
            LOG.info("Record reader closed.");
        }

    }
}
