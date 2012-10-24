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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.ExceptionsHelper;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParseException;

class ElasticSearchStreamingRecordWriter<K, V> implements RecordWriter<K, V> {

    static Log LOG = LogFactory.getLog(ElasticSearchStreamingRecordWriter.class);

    private String  defaultIndexName;
    private String  defaultTypeName;
    private String  indexFieldName;
    private String  typeFieldName;
    private String  idFieldName;
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

    // JSON parsing
    private              ObjectMapper       mapper;

    //
    // == Lifecycle ==
    // 

    public ElasticSearchStreamingRecordWriter(String defaultIndexName, String defaultTypeName, String indexFieldName, String typeFieldName, String idFieldName, Integer bulkSize) {
	this.defaultIndexName = defaultIndexName;
	this.defaultTypeName  = defaultTypeName;
	this.indexFieldName   = indexFieldName;
	this.typeFieldName    = typeFieldName;
	this.idFieldName      = idFieldName;
	this.bulkSize         = bulkSize;
	
	LOG.info("Writing "+Integer.toString(bulkSize)+" records per batch");
	LOG.info("Using default target /"+defaultIndexName+"/"+defaultTypeName);
	LOG.info("Records override default target with index field '"+indexFieldName+"', type field '"+typeFieldName+"', and ID field '"+idFieldName);
            
	startEmbeddedClient();
	this.currentRequest = client.prepareBulk();
	this.mapper = new ObjectMapper();
    }

    /**
       Start an embedded Elasticsearch client.  The client will not be
       a data node and will not store data locally.

       The client will connect to the target Elasticsearch cluster as
       a client node, enabling one-hop writes for all data.  See
       http://www.elasticsearch.org/guide/reference/java-api/client.html
    */
    private void startEmbeddedClient() {
	LOG.info("Starting embedded Elasticsearch client (non-datanode)...");
	this.node   = NodeBuilder.nodeBuilder().client(true).node();
	this.client = node.client();
	LOG.info("Successfully joined Elasticsearch cluster '"+ClusterName.clusterNameFromSettings(node.settings())+'"');
    }


    /**
       Close the Elasticsearch client, sending out one last bulk write
       if necessary.
    */
    public void close(Reporter reporter) throws IOException {
	sendBulkRequestIfMoreThan(0);
	LOG.info("Shutting down Elasticsearch client...");
	if (client != null) client.close();
	if (node   != null) node.close();
	LOG.info("Successfully shut down Elasticsearch client");
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
	if (record.containsKey(idFieldName)) {
	    Object idValue = record.get(idFieldName);
	    currentRequest.add(Requests.indexRequest(indexNameForRecord(record)).id(String.valueOf(idValue)).type(typeNameForRecord(record)).create(false).source(json));
	} else {
	    currentRequest.add(Requests.indexRequest(indexNameForRecord(record)).type(typeNameForRecord(record)).source(json));
	}
    }

    private String indexNameForRecord(Map<String, Object> record) {
	if (record.containsKey(indexFieldName)) {
	    Object indexValue   = record.get(indexFieldName);
	    return String.valueOf(indexValue);
	} else {
	    return defaultIndexName;
	}
    }

    private String typeNameForRecord(Map<String, Object> record) {
	if (record.containsKey(typeFieldName)) {
	    Object typeValue   = record.get(typeFieldName);
	    return String.valueOf(typeValue);
	} else {
	    return defaultTypeName;
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
