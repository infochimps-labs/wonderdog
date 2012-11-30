package com.infochimps.elasticsearch;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

import org.elasticsearch.search.Scroll;

import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;

public class ElasticSearchStreamingSplit  implements InputSplit, Writable {

    private String  indexName;
    private String  mappingName;
    private Integer numSplits;
    private String  queryJSON;
    private Long    numHits;
    private Integer from;
    private Integer size;

    public ElasticSearchStreamingSplit() {
    }

    public ElasticSearchStreamingSplit(String indexName , String mappingName, Integer numSplits, String queryJSON, Long numHits, Integer from, Integer size) {
	this.indexName   = indexName;
	this.mappingName = mappingName;
	this.numSplits   = numSplits;
	this.queryJSON   = queryJSON;
	this.numHits     = numHits;
	this.from        = from;
	this.size        = size;
    }

    public String getSummary() {
	Integer thisSplitNum  = (int) (((long) from) / (numHits / ((long) numSplits)));
	return "ElasticSearch input split "+String.valueOf(thisSplitNum + 1)+"/"+String.valueOf(numSplits)+" with "+String.valueOf(size)+" records from /"+indexName+"/"+mappingName;
    }

    public Integer getSize() {
	return size;
    }

    public boolean hasQuery() {
	return queryJSON != null && queryJSON.length() > 0;
    }

    public SearchRequestBuilder initialScrollRequest(Client client, Scroll scroll, Integer requestSize) {
	SearchRequestBuilder request = client.prepareSearch(indexName).setSearchType(SearchType.SCAN).setScroll(scroll);
	if (mappingName != null && mappingName.length() > 0) {
	    request.setTypes(mappingName);
	}
	request.setFrom((int) from);
	request.setSize(requestSize);
	if (hasQuery()) {
	    request.setQuery(queryJSON);
	}
	return request;
    }

    public SearchScrollRequestBuilder scrollRequest(Client client, Scroll scroll, String scrollId) {
	return client.prepareSearchScroll(scrollId).setScroll(scroll);
    }

    @Override
    public String[] getLocations() {
        return new String[] {};
    }
    
    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	this.indexName   = Text.readString(in);
	this.mappingName = Text.readString(in);
	this.numSplits   = in.readInt();
	this.queryJSON   = Text.readString(in);
	this.numHits     = in.readLong();
	this.from        = in.readInt();
	this.size        = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	Text.writeString(out, indexName);
	Text.writeString(out, mappingName);
	out.writeInt(numSplits);
	Text.writeString(out, queryJSON);
	out.writeLong(numHits);
	out.writeInt(from);
	out.writeInt(size);
    }
}
