package com.infochimps.elasticsearch;

import java.io.IOException;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.*;

import org.elasticsearch.common.unit.TimeValue;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterName;

class ElasticSearchStreamingRecordReader<K, V> implements RecordReader<K, V> {

    static Log LOG = LogFactory.getLog(ElasticSearchStreamingRecordReader.class);

    private static final String  ES_REQUEST_SIZE_OPT = "elasticsearch.input.request_size";
    private static final String  ES_REQUEST_SIZE     = "100";
    private              Integer requestSize;

    private static final String    ES_SCROLL_TIMEOUT_OPT = "elasticsearch.input.scroll_timeout";
    private static final String    ES_SCROLL_TIMEOUT     = "5m";
    private              String    scrollTimeout;
    private static final TimeValue defaultScrollTimeout = new TimeValue((long) 300000); // 5 minutes
    private              Scroll    scroll;
    
    private Node                        node;
    private Client                      client;
    private ElasticSearchStreamingSplit split;
    private              boolean transport;
    private              String  transportHost;
    private              Integer transportPort;
    
    private String  scrollId;
    private Integer recordsRead;
    private Iterator<SearchHit> hitsItr = null;
        
    public ElasticSearchStreamingRecordReader(InputSplit split, JobConf conf, boolean transport, String transportHost, Integer transportPort) {
	this.split         = (ElasticSearchStreamingSplit) split;
	this.recordsRead   = 0;
	this.requestSize   = Integer.parseInt(conf.get(ES_REQUEST_SIZE_OPT, ES_REQUEST_SIZE));
	this.scrollTimeout = conf.get(ES_SCROLL_TIMEOUT_OPT, ES_SCROLL_TIMEOUT);
	this.scroll        = new Scroll(TimeValue.parseTimeValue(this.scrollTimeout, defaultScrollTimeout));

	this.transport     = transport;
	this.transportHost = transportHost;
	this.transportPort = transportPort;
	
	LOG.info("Initializing "+this.split.getSummary());
	if (transport) {
	    this.client = buildTransportClient();
	} else {
	    startNode();
	    this.client = node.client();
	}
	fetchNextHits();
    }

    /**
       Build a transport client that will connect to some
       Elasticsearch node.
       
     */
    private Client buildTransportClient() {
	LOG.info("Connecting transport client to "+transportHost+":"+Integer.toString(transportPort));
	Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", "true").build();
	return new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(transportHost, transportPort));
    }

    /**
       Start an embedded Elasticsearch node.

       The node will not store any data locally (non-datanode) but
       will connect to a cluster using the default Elasticsearch
       settings (those available in
       /etc/elasticsearch/elasticsearch.yml).
    */
    private void startNode() {
	LOG.info("Starting embedded Elasticsearch client (non-datanode)...");
	this.node   = NodeBuilder.nodeBuilder().client(true).node();
	LOG.info("Successfully joined Elasticsearch cluster '"+ClusterName.clusterNameFromSettings(node.settings())+'"');
    }
    
    private void fetchNextHits() {
	if (scrollId == null) {
	    LOG.info("Running initial scroll with timeout "+scrollTimeout);
	    SearchRequestBuilder request  = split.initialScrollRequest(client, scroll, requestSize);
	    SearchResponse       response = request.execute().actionGet();
	    this.scrollId = response.getScrollId();
	    LOG.info("Got scroll ID "+scrollId);
	    // Do we need to call fetchNextHits() again here?  Or does
	    // the initial request also itself contain the first set
	    // of hits for the scroll?
	    // 
	    // fetchNextHits();
	} else {
	    // LOG.info("Running query for scroll ID "+scrollId+" with timeout "+scrollTimeout);
	    SearchScrollRequestBuilder request  = split.scrollRequest(client, scroll, scrollId);
	    SearchResponse             response = request.execute().actionGet();
	    this.scrollId = response.getScrollId();
	    // LOG.info("Got scroll ID "+scrollId);
	    this.hitsItr = response.getHits().iterator();
	}
    }

    @Override
	public boolean next(K key, V value) throws IOException {
	if (shouldReadAnotherRecord()) {
	    // We should read more records because we haven't read as
	    // many as we know to be in this split yet.
	    if (hasAnotherRecord()) {
		// We already have records stacked up ready to read.
		readRecord(key, value);
		return true;
	    } else {
		// We don't have records stacked up so we might need
		// to fetch some more hits.
		fetchNextHits();
		if (hasAnotherRecord()) {
		    // Now if we have records we read one
		    readRecord(key, value);
		    return true;
		} else {
		    // But if no records are here this time, it's
		    // because we know we're done reading the input.
		    return false;
		}
	    }
	} else {
	    // Return false as we're done with this split.
	    return false;
	}
    }

    private boolean shouldReadAnotherRecord() {
	return recordsRead < split.getSize();
    }
    
    private boolean hasAnotherRecord() {
	return hitsItr != null && hitsItr.hasNext();
    }

    private void readRecord(K key, V value) {
	SearchHit hit = hitsItr.next();
	if (hit != null) {
	    Text keyText   = (Text) key;
	    Text valueText = (Text) value;
	    keyText.set(hit.sourceAsString());
	    valueText.set(hit.sourceAsString());
	    recordsRead += 1;
	}
    }
    
    @Override
	public K createKey() {
	return (K) new Text();
    }

    @Override
	public V createValue() {
	return (V) new Text();
    }
	
    @Override
	public long getPos() throws IOException {
	return recordsRead;
    }
        
    @Override
	public float getProgress() throws IOException {
	return ((float) recordsRead) / ((float) split.getSize());
    }

    @Override
	public void close() throws IOException {
	if (client != null) {
	    LOG.info("Shutting down Elasticsearch client...");
	    client.close();
	}
	if (node != null) {
	    LOG.info("Shutting down Elasticsearch node...");
	    node.close();
	}
    }
    
}
