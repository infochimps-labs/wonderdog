package com.infochimps.elasticsearch.pig;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.UDFContext;


import com.google.common.collect.Lists;

import com.infochimps.elasticsearch.ElasticSearchOutputFormat;

public class ElasticSearchIndex extends StoreFunc implements StoreFuncInterface {

    protected RecordWriter writer = null;
    protected String idField;
    protected String bulkSize;

    private static final Log LOG = LogFactory.getLog(ElasticSearchIndex.class);

    public ElasticSearchIndex(String idField, String bulkSize) throws IOException {
        this.idField  = idField;
        this.bulkSize = bulkSize;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        String fieldNames   = "";       
        for (String field : s.fieldNames()) {
            fieldNames += field;
            fieldNames += ",";
        }
        property.setProperty("elasticsearch.pig.field.names", fieldNames);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        LOG.info("Setting location...");
        LOG.info("location = "+location);
        String[] es_store  = location.substring(5).split("/");
        if (es_store.length != 2) {
            throw new RuntimeException("Please specify a valid elasticsearch index, eg. es://myindex/myobj");
        }
        Configuration conf = job.getConfiguration();
        try {
            conf.set("elasticsearch.index.name", es_store[0]);
            conf.set("elasticsearch.object.type", es_store[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new RuntimeException("You must specify both an index and an object type.");
        }
        conf.set("elasticsearch.bulk.size", bulkSize);
        conf.set("elasticsearch.id.field", idField);
        LOG.info("id field = "+conf.get("elasticsearch.id.field"));
        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        conf.set("elasticsearch.field.names", property.getProperty("elasticsearch.pig.field.names"));
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        LOG.info("Getting output format...");
        return new ElasticSearchOutputFormat();
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        LOG.info("Preparing to write...");
        this.writer = writer;
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        MapWritable record  = new MapWritable();
        String[] fieldNames = property.getProperty("elasticsearch.pig.field.names").split(",");
        for (int i = 0; i < t.size(); i++) {
            if (i < fieldNames.length) {
                try {
                    record.put(new Text(fieldNames[i]), new Text(t.get(i).toString()));
                } catch (NullPointerException e) {
                    LOG.info("fixme. trying to index null field, should just increment a hadoop counter instead");
                }
            }
        }
        try {
            writer.write(NullWritable.get(), record);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {

    }

}
