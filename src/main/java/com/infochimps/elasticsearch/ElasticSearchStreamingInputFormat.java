package com.infochimps.elasticsearch;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.elasticsearch.common.settings.loader.YamlSettingsLoader;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.action.search.SearchRequestBuilder;

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
    
    private static final String ES_MAPPING_OPT     = "elasticsearch.input.mapping";
    private static final String ES_DEFAULT_MAPPING = "streaming_record";
    private              String mappingName;

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

    private static final String ES_UNICAST_HOSTS_NAME  = "discovery.zen.ping.unicast.hosts";
    
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
	this.mappingName    = conf.get(ES_MAPPING_OPT,  ES_DEFAULT_MAPPING);
        // this.numSplits   = Integer.parseInt(conf.get(ES_NUM_SPLITS_OPT, ES_NUM_SPLITS));
        this.queryJSON   = conf.get(ES_QUERY_OPT, ES_QUERY);
	String message = "Using input /"+indexName;
	if (mappingName != null && mappingName.length() > 0) {
	    message += "/"+mappingName;
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
	this.client = new TransportClient();
	Map<String,String> settings = parsedSettings(conf);
	String host = hostname(settings);
	if (host.toString().length() == 0) {
	    System.exit(1);
	}
	LOG.info("Attempting to connect to Elasticsearch node at " + host + ":9300");
	this.client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(host, 9300));
	LOG.info("Connected to Elasticsearch cluster");
    }

    private Map<String,String> parsedSettings(JobConf conf) {
	String esConfigPath  = conf.get(ES_CONFIG_OPT,  ES_CONFIG);
	String esPluginsPath = conf.get(ES_PLUGINS_OPT, ES_PLUGINS);
	
	try {
	    BufferedReader reader = new BufferedReader( new FileReader(esConfigPath));
	    String         line = null;
	    StringBuilder  stringBuilder = new StringBuilder();
	    String         ls = System.getProperty("line.separator");
	    while( ( line = reader.readLine() ) != null ) {
		stringBuilder.append( line );
		stringBuilder.append( ls );
	    }
	    return new YamlSettingsLoader().load(stringBuilder.toString());
	} catch (IOException e) {
	    LOG.error("Could not find or read the configuration file " + esConfigPath + ".");
	    return new HashMap<String,String>();
	}
    }

    private String hostname(Map<String,String> settings) {
	String hostsString = settings.get(ES_UNICAST_HOSTS_NAME);
	if (hostsString.toString().length() == 0) {
	    LOG.error("Could not find hosts. Did you set the '" + ES_UNICAST_HOSTS_NAME + "' key?");
	    return "";
	}
	
	String[] hosts = hostsString.split(",");
	if (hosts.length > 0) {
	    String host = hosts[0];
	    if (host.toString().length() == 0) {
		LOG.error("Could not parse hosts from '" + ES_UNICAST_HOSTS_NAME + "' key.");
		return "";
	    } else {
		return host;
	    }
	} else {
	    LOG.error("Could not find any hosts in the '" + ES_UNICAST_HOSTS_NAME + "' key.");
	    return "";
	}
    }

    private void stopTransportClient() {
	if (client != null) client.close();
	LOG.info("Disconnected from Elasticsearch cluster");
    }

    private void findNumHits() {
	SearchRequestBuilder request = client.prepareSearch(indexName);
	if (mappingName != null && mappingName.length() > 0) {
	    request.setTypes(mappingName);
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
	// Say that
	//
	//   numHits   = 7
	//   numSplits = 2
        if((long) numSplits > numHits) {
	    numSplits = (int) numHits;
	}

	this.recordsPerSplit = (int) (numHits/((long)numSplits)); // == 3 records/split
	
        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);

	// i == 0, 1
        for(int i = 0; i < numSplits; i++) {
	    Integer from = i * recordsPerSplit;
            splits.add(new ElasticSearchStreamingSplit(indexName, mappingName, numSplits, queryJSON, numHits, from, recordsPerSplit));
        }
	// 7 is > (2 * 3) == 6
        if (numHits > ((long) (numSplits * recordsPerSplit))) {
	    Integer from = numSplits * recordsPerSplit;
	    Integer size = (int) (numHits - ((long) from));
	    splits.add(new ElasticSearchStreamingSplit(indexName, mappingName, numSplits, queryJSON, numHits, from, size));
	}

	LOG.info("Splitting "+String.valueOf(numHits)+" hits across "+String.valueOf(splits.size())+" splits ("+String.valueOf(recordsPerSplit)+" hits/split)");

        return splits.toArray(new InputSplit[splits.size()]);
    }

}
