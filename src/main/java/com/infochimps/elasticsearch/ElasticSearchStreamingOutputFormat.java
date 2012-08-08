package com.infochimps.elasticsearch;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.*;
import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;

/**
   
   Hadoop OutputFormat for writing arbitrary MapWritables (essentially
   HashMaps) into Elasticsearch. Records are batched up and sent in a
   one-hop manner to the elastic search data nodes that will index
   them.
   
*/
public class ElasticSearchStreamingOutputFormat<K, V> implements OutputFormat<K, V> {
    
    static Log LOG = LogFactory.getLog(ElasticSearchStreamingOutputFormat.class);

    // Job settings we need to control directly from Java options.
    private static final String ES_INDEX_OPT     = "elasticsearch.output.index";
    private static final String ES_DEFAULT_INDEX = "hadoop";
    private              String defaultIndexName;
	
    private static final String ES_TYPE_OPT     = "elasticsearch.output.type";
    private static final String ES_DEFAULT_TYPE = "streaming_record";
    private              String defaultTypeName;

    private static final String ES_INDEX_FIELD_OPT = "elasticsearch.output.index.field";
    private static final String ES_INDEX_FIELD     = "_index";
    private              String indexFieldName;

    private static final String ES_TYPE_FIELD_OPT = "elasticsearch.output.type.field";
    private static final String ES_TYPE_FIELD     = "_type";
    private              String typeFieldName;
	
    private static final String ES_ID_FIELD_OPT = "elasticsearch.output.id.field";
    private static final String ES_ID_FIELD     = "_id";
    private              String idFieldName;
	
    private static final String ES_BULK_SIZE_OPT     = "elasticsearch.output.bulk_size";
    private static final String ES_BULK_SIZE         = "100";
    private              int    bulkSize;


    // Elasticsearch internal settings required to make a client
    // connection.
    private static final String ES_CONFIG_OPT        = "es.config";
    private static final String ES_CONFIG            = "/etc/elasticsearch/elasticsearch.yml";
    
    private static final String ES_PLUGINS_OPT       = "es.path.plugins";
    private static final String ES_PLUGINS           = "/usr/local/share/elasticsearch/plugins";
    
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException {
	setLocalElasticSearchInstallation(conf);
	String  defaultIndexName = conf.get(ES_INDEX_OPT,       ES_DEFAULT_INDEX);
	String  defaultTypeName  = conf.get(ES_TYPE_OPT,        ES_DEFAULT_TYPE);
	String  indexFieldName   = conf.get(ES_INDEX_FIELD_OPT, ES_INDEX_FIELD);
	String  typeFieldName    = conf.get(ES_TYPE_FIELD_OPT,  ES_TYPE_FIELD);
	String  idFieldName      = conf.get(ES_ID_FIELD_OPT,    ES_ID_FIELD);
	Integer bulkSize         = Integer.parseInt(conf.get(ES_BULK_SIZE_OPT, ES_BULK_SIZE));
        return (RecordWriter) new ElasticSearchStreamingRecordWriter(defaultIndexName, defaultTypeName, indexFieldName, typeFieldName, idFieldName, bulkSize);
    }
    
    public void setLocalElasticSearchInstallation(JobConf conf) {
	String esConfigPath  = conf.get(ES_CONFIG_OPT,  ES_CONFIG);
	String esPluginsPath = conf.get(ES_PLUGINS_OPT, ES_PLUGINS);
	System.setProperty(ES_CONFIG_OPT,esConfigPath);
	System.setProperty(ES_PLUGINS_OPT,esPluginsPath);
	LOG.info("Using Elasticsearch configuration file at "+esConfigPath+" and plugin directory "+esPluginsPath);
    }

    public ElasticSearchStreamingOutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchStreamingOutputCommitter();
    }
    
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    }
}
