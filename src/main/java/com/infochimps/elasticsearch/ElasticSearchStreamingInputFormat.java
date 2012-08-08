package com.infochimps.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.client.action.search.SearchRequestBuilder;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilders.*;
import org.elasticsearch.cluster.ClusterName;

public class ElasticSearchStreamingInputFormat<K, V> implements InputFormat<K, V> {

    static Log LOG = LogFactory.getLog(ElasticSearchStreamingInputFormat.class);
    
    // Job settings we need to control directly from Java options.
    private static final String ES_INDEX_OPT     = "elasticsearch.input.index";
    private static final String ES_DEFAULT_INDEX = "hadoop";
    private              String indexName;
    
    private static final String ES_TYPE_OPT     = "elasticsearch.input.type";
    private              String typeName;

    private static final String  ES_NUM_SPLITS_OPT = "elasticsearch.input.splits";
    private static final String  ES_NUM_SPLITS     = "1";
    private              Integer numSplits;
    
    private static final String ES_QUERY_OPT = "elasticsearch.input.query";
    private static final String ES_QUERY     = "{\"match_all\": {}}";
    private              String queryJSON;

    // Calculated after the first query.
    private long       numHits;
    private Integer    recordsPerSplit;
    
    // Elasticsearch internal settings required to make a client
    // connection.
    private static final String ES_CONFIG_OPT = "es.config";
    private static final String ES_CONFIG     = "/etc/elasticsearch/elasticsearch.yml";
    
    private static final String ES_PLUGINS_OPT = "es.path.plugins";
    private static final String ES_PLUGINS     = "/usr/local/share/elasticsearch/plugins";
    
    private TransportClient client;

    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) {
	setLocalElasticSearchInstallation(conf);
        return (RecordReader) new ElasticSearchStreamingRecordReader(split, conf);
    }

    public InputSplit[] getSplits(JobConf conf, int requestedNumSplits) {
	this.numSplits = requestedNumSplits;
	
	setLocalElasticSearchInstallation(conf);
	parseInput(conf);
	
        startTransportClient(conf);
	findNumHits();
	stopTransportClient();
	
	return createSplits();
    }


    //
    // == Setup ==
    //

    public void setLocalElasticSearchInstallation(JobConf conf) {
	String esConfigPath  = conf.get(ES_CONFIG_OPT,  ES_CONFIG);
	String esPluginsPath = conf.get(ES_PLUGINS_OPT, ES_PLUGINS);
	System.setProperty(ES_CONFIG_OPT, esConfigPath);
	System.setProperty(ES_PLUGINS_OPT,esPluginsPath);
	LOG.info("Using Elasticsearch configuration file at "+esConfigPath+" and plugin directory "+esPluginsPath);
    }
    
    private void parseInput(JobConf conf) {
	this.indexName   = conf.get(ES_INDEX_OPT, ES_DEFAULT_INDEX);
	this.typeName    = conf.get(ES_TYPE_OPT,  "");
        // this.numSplits   = Integer.parseInt(conf.get(ES_NUM_SPLITS_OPT, ES_NUM_SPLITS));
        this.queryJSON   = conf.get(ES_QUERY_OPT, ES_QUERY);
	String message = "Using input /"+indexName;
	if (typeName != null && typeName.length() > 0) {
	    message += "/"+typeName;
	}
	if (queryJSON != null && queryJSON.length() > 0) {
	    message += " with query: "+queryJSON;
	}
	LOG.info(message);
    }

    //
    // == Connecting to Elasticsearch and Querying ==
    // 

    private void startTransportClient(JobConf conf) {
	String esConfigPath  = conf.get(ES_CONFIG_OPT,  ES_CONFIG);
	String esPluginsPath = conf.get(ES_PLUGINS_OPT, ES_PLUGINS);
	
	Settings settings = ImmutableSettings.settingsBuilder()
	    .put(ES_CONFIG_OPT,  esConfigPath)
	    .put(ES_PLUGINS_OPT, esPluginsPath)
	    .build();

	// FIXME -- can't figure out how to get settings from
	// elasticsearch.yml to control TransportClient.
	this.client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("10.124.115.89", 9300));

	LOG.info("Connected to Elasticsearch cluster");
    }

    private void stopTransportClient() {
	if (client != null) client.close();
	LOG.info("Disconnected from Elasticsearch cluster");	
    }

    private void findNumHits() {
	SearchRequestBuilder request = client.prepareSearch(indexName);
	if (typeName != null && typeName.length() > 0) {
	    request.setTypes(typeName);
	}
	request.setSearchType(SearchType.COUNT);
	if (queryJSON != null && queryJSON.length() > 0) {
	    request.setQuery(queryJSON);
	}
        SearchResponse response = request.execute().actionGet();
        this.numHits = response.hits().totalHits();

	LOG.info("Ran query: "+String.valueOf(numHits)+" hits");
    }

    //
    // == Setting splits ==
    // 
    
    private void readjustSplitsByHits() {

    }

    private InputSplit[] createSplits() {
	// This could be bad
        if((long) numSplits > numHits) {
	    numSplits = (int) numHits;
	}

	this.recordsPerSplit = (int) (numHits/((long)numSplits));	
	
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
        for(int i = 0; i < numSplits; i++) {
	    Integer from = i * recordsPerSplit;
            Integer size = (recordsPerSplit == 1) ? 1 : recordsPerSplit;
            splits.add(new ElasticSearchStreamingSplit(indexName, typeName, numSplits, queryJSON, numHits, from, size));
        }
        if (numHits > ((long) (numSplits * recordsPerSplit))) {
	    Integer from = numSplits * recordsPerSplit;
	    Integer size = (int) (numHits - ((long) from));
	    splits.add(new ElasticSearchStreamingSplit(indexName, typeName, numSplits, queryJSON, numHits, from, size));
	}

	LOG.info("Splitting "+String.valueOf(numHits)+" hits across "+String.valueOf(numSplits)+" splits ("+String.valueOf(recordsPerSplit)+" hits/split)");

        return splits.toArray(new InputSplit[splits.size()]);
    }

}
