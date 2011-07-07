package com.infochimps.elasticsearch;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class ElasticSearchSplit extends InputSplit implements Writable {

    private String queryString;
    private long from;
    private long size;

    public ElasticSearchSplit() {}
    
    public ElasticSearchSplit(String queryString, long from, long size) {
        this.queryString = queryString;
        this.from = from;
        this.size = size;
    }

    public String getQueryString() {
        return queryString;
    }
    
    public long getFrom() {
        return from;
    }

    public long getSize() {
        return size;
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
        queryString = Text.readString(in);
        from = in.readLong();
        size = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, queryString);
        out.writeLong(from);
        out.writeLong(size);
    }
}
