package com.infochimps.elasticsearch;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class ElasticSearchSplit extends InputSplit implements Writable {
    public ElasticSearchSplit() {
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
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }
}
