package com.infochimps.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.ExceptionsHelper;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParseException;

class ElasticSearchStreamingRecordWriter<K, V> implements RecordWriter<K, V> {

    static Log LOG = LogFactory.getLog(ElasticSearchStreamingRecordWriter.class);

    private String  defaultIndexName;
    private String  defaultMappingName;
    private String  indexFieldName;
    private String  mappingFieldName;
    private String  idFieldName;
    private String  routingFieldName;
    private Integer bulkSize;

    // Bookkeeping
    private AtomicLong totalBulkTime  = new AtomicLong();
    private AtomicLong totalBulkItems = new AtomicLong();
    private Random     randgen        = new Random();
    private long       runStartTime   = System.currentTimeMillis();

    // Elasticsearch indexing
    private              Node               node;
    private              Client             client;
    private volatile     BulkRequestBuilder currentRequest;
    private              boolean transport;
    private              String  transportHost;
    private              Integer transportPort;

    // JSON parsing
    private              ObjectMapper       mapper;

    //
    // == Lifecycle ==
    // 

    public ElasticSearchStreamingRecordWriter(String defaultIndexName, String defaultMappingName, String indexFieldName, String mappingFieldName, String idFieldName, String routingFieldName, Integer bulkSize, boolean transport, String transportHost, Integer transportPort) {
	this.defaultIndexName   = defaultIndexName;
	this.defaultMappingName = defaultMappingName;
	this.indexFieldName     = indexFieldName;
	this.mappingFieldName   = mappingFieldName;
	this.idFieldName        = idFieldName;
	this.routingFieldName   = routingFieldName;
	this.bulkSize           = bulkSize;
	this.transport          = transport;
	this.transportHost      = transportHost;
	this.transportPort      = transportPort;
	
	LOG.info("Writing "+Integer.toString(bulkSize)+" records per batch");
	LOG.info("Using default target /"+defaultIndexName+"/"+defaultMappingName);
	LOG.info("Records override default target with index field '"+indexFieldName+"', mapping field '"+mappingFieldName+"', and ID field '"+idFieldName);
	if (transport) {
	    this.client = buildTransportClient();
	} else {
	    startNode();
	    this.client = node.client();
	}
	this.currentRequest = client.prepareBulk();
	this.mapper = new ObjectMapper();
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


    /**
       Close the Elasticsearch client, sending out one last bulk write
       if necessary.
    */
    public void close(Reporter reporter) throws IOException {
	sendBulkRequestIfMoreThan(0);
	if (client != null) {
	    LOG.info("Shutting down Elasticsearch client...");
	    client.close();
	}
	if (node != null) {
	    LOG.info("Shutting down Elasticsearch node...");
	    node.close();
	}
    }

    //
    // == Writing records == 
    // 
    
    public void write(K key, V value) throws IOException {
	String json = ((Text) key).toString();
	try {
	    index(json);
	    sendBulkRequestIfBigEnough();
	} catch(Exception e) {
	    if (ExceptionsHelper.unwrapCause(e) instanceof JsonParseException) {
		LOG.debug("Bad record: "+json);
		return;
	    } else {
		LOG.error("Could not write record: "+json, e);
	    }
	}
    }

    private void index(String json) throws IOException {
	Map<String, Object> record = mapper.readValue(json, Map.class);
	IndexRequest request = null;
	if (record.containsKey(idFieldName)) {
	    Object idValue = record.get(idFieldName);
	     request = Requests.indexRequest(indexNameForRecord(record)).id(String.valueOf(idValue)).type(mappingNameForRecord(record)).create(false).source(json);
	} else {
	    request = Requests.indexRequest(indexNameForRecord(record)).type(mappingNameForRecord(record)).source(json);
	}
	if (record.containsKey(routingFieldName)) {
	    Object routingValue = record.get(routingFieldName);
		request.routing(String.valueOf(routingValue));
	}
	currentRequest.add(request);
    }

    private String indexNameForRecord(Map<String, Object> record) {
	if (record.containsKey(indexFieldName)) {
	    Object indexValue   = record.get(indexFieldName);
	    return String.valueOf(indexValue);
	} else {
	    return defaultIndexName;
	}
    }

    private String mappingNameForRecord(Map<String, Object> record) {
	if (record.containsKey(mappingFieldName)) {
	    Object mappingValue   = record.get(mappingFieldName);
	    return String.valueOf(mappingValue);
	} else {
	    return defaultMappingName;
	}
    }

    //
    // == Bulk request handling ==
    //
    
    private void sendBulkRequestIfBigEnough() {
	sendBulkRequestIfMoreThan(bulkSize);
    }

    private void sendBulkRequestIfMoreThan(int size) {
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
