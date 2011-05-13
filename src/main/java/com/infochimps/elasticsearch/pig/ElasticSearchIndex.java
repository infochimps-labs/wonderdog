package com.infochimps.elasticsearch.pig;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.net.URI;

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
import org.apache.hadoop.filecache.DistributedCache;

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

import com.infochimps.elasticsearch.ElasticSearchOutputFormat;

/**
   Pig storefunc for Elastic Search. Takes tuples of any primitive type, converts them
   to strings, and indexes them.

   USAGE:

   STORE records INTO ElasticSearchIndex();
   STORE records INTO ElasticSearchIndex(idField, bulkSize);
   STORE records INTO ElasticSearchIndex(idField, bulkSize, esConfig);
   STORE records INTO ElasticSearchIndex(idField, bulkSize, esConfig, esPlugins);

   where:

   idField   = Which field of the record to use as the record id. If none is passed in
               then the record is assumed to have no id.
   bulkSize  = Number of records for ElasticSearchOutputFormat to batch up before sending
               a bulk index request to Elastic Search. Default: 1000.
   esConfig  = Full path to elasticsearch.yml. Default: /etc/elasticsearch/elasticsearch.yml
   esPlugins = Full path to elastic search plugins dir. Default: /usr/local/share/elasticsearch/plugins
   
 */
public class ElasticSearchIndex extends StoreFunc implements StoreFuncInterface {

    private static final Log LOG = LogFactory.getLog(ElasticSearchIndex.class);
    
    protected RecordWriter writer = null;
    protected String idField;
    protected String bulkSize;
    protected String esConfig;
    protected String esPlugins;

    // For hadoop configuration
    private static final String ES_INDEX_NAME = "elasticsearch.index.name";
    private static final String ES_BULK_SIZE = "elasticsearch.bulk.size";
    private static final String ES_IS_JSON = "elasticsearch.is_json";
    private static final String ES_ID_FIELD_NAME = "elasticsearch.id.field.name";
    private static final String ES_FIELD_NAMES = "elasticsearch.field.names";
    private static final String ES_ID_FIELD = "elasticsearch.id.field";
    private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
    private static final String ES_CONFIG = "elasticsearch.config";
    private static final String ES_PLUGINS = "elasticsearch.plugins.dir";
    private static final String PIG_ES_FIELD_NAMES = "elasticsearch.pig.field.names";

    // Other string constants
    private static final String SLASH = "/";
    private static final String COMMA = ",";
    private static final String LOCAL_SCHEME = "file://";
    private static final String NO_ID_FIELD = "-1";
    private static final String DEFAULT_BULK = "1000";
    private static final String DEFAULT_ES_CONFIG = "/etc/elasticsearch/elasticsearch.yml";
    private static final String DEFAULT_ES_PLUGINS = "/usr/local/share/elasticsearch/plugins";
    
    public ElasticSearchIndex() {
        this(NO_ID_FIELD, DEFAULT_BULK);
    }

    public ElasticSearchIndex(String idField, String bulkSize) {
        this(idField, bulkSize, DEFAULT_ES_CONFIG);
    }

    public ElasticSearchIndex(String idField, String bulkSize, String esConfig) {
        this(idField, bulkSize, esConfig, DEFAULT_ES_PLUGINS);
    }

    public ElasticSearchIndex(String idField, String bulkSize, String esConfig, String esPlugins) {
        this.idField   = idField;
        this.bulkSize  = bulkSize;
        this.esConfig  = esConfig;
        this.esPlugins = esPlugins;
    }

    /**
       Check that schema is reasonable and serialize the field names as a string for later use.
     */
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        String fieldNames   = "";       
        for (String field : s.fieldNames()) {
            fieldNames += field;
            fieldNames += COMMA;
        }
        property.setProperty(PIG_ES_FIELD_NAMES, fieldNames);
    }

    /**
       Look at passed in location and configuration and set options. Note that, since this
       is called more than once, we need to make sure and not change anything we've already
       set.
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        String[] es_store  = location.substring(5).split(SLASH);
        if (es_store.length != 2) {
            throw new RuntimeException("Please specify a valid elasticsearch index, eg. es://myindex/myobj");
        }
        Configuration conf = job.getConfiguration();
        // Only set if we haven't already
        if (conf.get(ES_INDEX_NAME) == null) {
            try {
                job.getConfiguration().set(ES_INDEX_NAME, es_store[0]);
                job.getConfiguration().set(ES_OBJECT_TYPE, es_store[1]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new RuntimeException("You must specify both an index and an object type.");
            }
            job.getConfiguration().setBoolean(ES_IS_JSON, false);
            job.getConfiguration().set(ES_BULK_SIZE, bulkSize);
            job.getConfiguration().set(ES_ID_FIELD, idField);
            
            //
            // FIXME! This needs to use the distributed cache
            //
            job.getConfiguration().set(ES_PLUGINS, esPlugins);
            //
            //
            //

            // Adds the elasticsearch.yml file (esConfig) to the distributed cache
            try {
                DistributedCache.addCacheFile(new URI(LOCAL_SCHEME+esConfig), job.getConfiguration());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            UDFContext context  = UDFContext.getUDFContext();
            Properties property = context.getUDFProperties(ResourceSchema.class);
            job.getConfiguration().set(ES_FIELD_NAMES, property.getProperty(PIG_ES_FIELD_NAMES));
        }
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new ElasticSearchOutputFormat();
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    /**
       Map a tuple object into a map-writable object for elasticsearch.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        MapWritable record  = new MapWritable();
        String[] fieldNames = property.getProperty(PIG_ES_FIELD_NAMES).split(COMMA);
        for (int i = 0; i < t.size(); i++) {
            if (i < fieldNames.length) {
                try {
                    record.put(new Text(fieldNames[i]), new Text(t.get(i).toString()));
                } catch (NullPointerException e) {
                    //LOG.info("Increment null field counter.");
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
