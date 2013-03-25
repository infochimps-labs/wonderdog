package com.infochimps.elasticsearch.pig;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.io.*;

import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.infochimps.elasticsearch.ElasticSearchOutputFormat;
import com.infochimps.elasticsearch.ElasticSearchInputFormat;
import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;

public class ElasticSearchStorage extends LoadFunc implements StoreFuncInterface {

    private String contextSignature = null;
    private RecordReader reader;
    protected RecordWriter writer = null;
    protected ObjectMapper mapper = new ObjectMapper();
    protected String esConfig;
    protected String esPlugins;

    // For hadoop configuration
    private static final String ES_INDEX_NAME = "elasticsearch.index.name";
    private static final String ES_BULK_SIZE = "elasticsearch.bulk.size";
    private static final String ES_ID_FIELD_NAME = "elasticsearch.id.field.name";
    private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
    private static final String ES_IS_JSON = "elasticsearch.is_json";
	private static final String ES_IS_AVRO = "elasticsearch.is_avro";

    private static final String PIG_ES_FIELD_NAMES = "elasticsearch.pig.field.names";
    private static final String ES_REQUEST_SIZE = "elasticsearch.request.size";
    private static final String ES_NUM_SPLITS = "elasticsearch.num.input.splits";
    private static final String ES_QUERY_STRING = "elasticsearch.query.string";
    
    private static final String COMMA = ",";
    private static final String LOCAL_SCHEME = "file://";
    private static final String DEFAULT_BULK = "1000";
    private static final String DEFAULT_ES_CONFIG = "/etc/elasticsearch/elasticsearch.yml";
    private static final String DEFAULT_ES_PLUGINS = "/usr/local/share/elasticsearch/plugins";
    private static final String ES_CONFIG_HDFS_PATH = "/tmp/elasticsearch/elasticsearch.yml";
    private static final String ES_PLUGINS_HDFS_PATH = "/tmp/elasticsearch/plugins";
    private static final String ES_CONFIG = "es.config";
    private static final String ES_PLUGINS = "es.path.plugins";
    
    public ElasticSearchStorage() {
        this(DEFAULT_ES_CONFIG, DEFAULT_ES_PLUGINS);
    }

    public ElasticSearchStorage(String esConfig) {
        this(esConfig, DEFAULT_ES_PLUGINS);
    }

    public ElasticSearchStorage(String esConfig, String esPlugins) {
        this.esConfig  = esConfig;
        this.esPlugins = esPlugins;
    }

    @Override
        public Tuple getNext() throws IOException {
        try {
            Tuple tuple = TupleFactory.getInstance().newTuple(2);
            if (reader.nextKeyValue()) {
                Text docId = (Text)reader.getCurrentKey();
                Text docContent = (Text)reader.getCurrentValue();
                tuple.set(0, new DataByteArray(docId.toString()));
                tuple.set(1, new DataByteArray(docContent.toString()));
                return tuple;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }

    @Override
    public InputFormat getInputFormat() {
        return new ElasticSearchInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        elasticSearchSetup(location, job);
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        return location;
    }
    
    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        return location;
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new ElasticSearchOutputFormat();
    }

    /**
       Here we set the field names for a given tuple even if we 
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

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    /**
       Here we handle both the delimited record case and the json case.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {

        UDFContext context  = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(ResourceSchema.class);
        MapWritable record  = new MapWritable();

        String isJson = property.getProperty(ES_IS_JSON);
		String isAvro = property.getProperty(ES_IS_AVRO);

		// Handle avro records (ie. isAvro == true)
		if (isAvro != null && isAvro.equals("true")) {
			String[] fieldNames = property.getProperty(PIG_ES_FIELD_NAMES)
					.split(COMMA);
			record = tupleToWritable(t, fieldNames);

		} else
		// Format JSON
		// Handle json records (ie. isJson == true)
		if (isJson != null && isJson.equals("true")) {
            if (!t.isNull(0)) {                
                String jsonData = t.get(0).toString();
                // parse json data and put into mapwritable record
                try {
					HashMap<String, Object> data = mapper.readValue(jsonData,
							HashMap.class);
                    record = (MapWritable)toWritable(data);
                } catch (JsonParseException e) {
                    e.printStackTrace();
                } catch (JsonMappingException e) {
                    e.printStackTrace();
                }
            }

		} else {
			// Handle delimited records (ie. isJson == false, isAvro == false)
			String[] fieldNames = property.getProperty(PIG_ES_FIELD_NAMES)
					.split(COMMA);
			for (int i = 0; i < t.size(); i++) {
				if (i < fieldNames.length) {
					try {
						record.put(new Text(fieldNames[i]), new Text(t.get(i)
								.toString()));
					} catch (NullPointerException e) {
						// LOG.info("Increment null field counter.");
					}
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
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;        
    }

    /**
       Pull out the elasticsearch setup code
    */
    private void elasticSearchSetup(String location, Job job) {
        // Need to use the uri parsing library here to pull out everything
        try {

            // Parse the passed in location URI, pulling out the arguments as well
            URI parsedLocation = new URI(location);
            HashMap<String, String> query = parseURIQuery(parsedLocation.getQuery());

            String esHost = location.substring(5).split("/")[0];
            if (esHost==null) {
                throw new RuntimeException("Missing elasticsearch index name, URI must be formatted as es://<index_name>/<object_type>?<params>");
            }

            if (parsedLocation.getPath()==null) {
                throw new RuntimeException("Missing elasticsearch object type, URI must be formatted as es://<index_name>/<object_type>?<params>");
            } 
            
            Configuration conf = job.getConfiguration();
            if (conf.get(ES_INDEX_NAME) == null) {

                // Set elasticsearch index and object type in the Hadoop configuration
                job.getConfiguration().set(ES_INDEX_NAME, esHost);
                job.getConfiguration().set(ES_OBJECT_TYPE, parsedLocation.getPath().replaceAll("/", ""));

                // Set the request size in the Hadoop configuration
                String requestSize = query.get("size");
                if (requestSize == null) requestSize = DEFAULT_BULK;
                job.getConfiguration().set(ES_BULK_SIZE, requestSize);
                job.getConfiguration().set(ES_REQUEST_SIZE, requestSize);
                
                // Set the id field name in the Hadoop configuration
                String idFieldName = query.get("id");
                if (idFieldName == null) idFieldName = "-1";
                job.getConfiguration().set(ES_ID_FIELD_NAME, idFieldName);

                String queryString = query.get("q");
                if (queryString==null) queryString = "*";
                job.getConfiguration().set(ES_QUERY_STRING, queryString);

                String numTasks = query.get("tasks");
                if (numTasks==null) numTasks = "100";
                job.getConfiguration().set(ES_NUM_SPLITS, numTasks);

                // Adds the elasticsearch.yml file (esConfig) and the plugins directory (esPlugins) to the distributed cache
                try {
                    Path hdfsConfigPath = new Path(ES_CONFIG_HDFS_PATH);
                    Path hdfsPluginsPath = new Path(ES_PLUGINS_HDFS_PATH);
                    
                    HadoopUtils.uploadLocalFileIfChanged(new Path(LOCAL_SCHEME+esConfig), hdfsConfigPath, job.getConfiguration());
                    HadoopUtils.shipFileIfNotShipped(hdfsConfigPath, job.getConfiguration());
                
                    HadoopUtils.uploadLocalFileIfChanged(new Path(LOCAL_SCHEME+esPlugins), hdfsPluginsPath, job.getConfiguration());
                    HadoopUtils.shipArchiveIfNotShipped(hdfsPluginsPath, job.getConfiguration());
                
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                //
                // This gets set even when loading data from elasticsearch
                //
                String isJson = query.get("json");
                if (isJson==null || isJson.equals("false")) {
                    // We're dealing with delimited records
                    UDFContext context  = UDFContext.getUDFContext();
					Properties property = context
							.getUDFProperties(ResourceSchema.class);
					property.setProperty(ES_IS_JSON, "false");
				}

				String isAvro = query.get("avro");
				if (isAvro != null || isJson.equals("true")) {
					// We're dealing with avro records
					UDFContext context = UDFContext.getUDFContext();
					Properties property = context
							.getUDFProperties(ResourceSchema.class);
					property.setProperty(ES_IS_AVRO, "true");
                    property.setProperty(ES_IS_JSON, "false");
                }

                // Need to set this to start the local instance of elasticsearch
                job.getConfiguration().set(ES_CONFIG, esConfig);
                job.getConfiguration().set(ES_PLUGINS, esPlugins);
            }            
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
       Look at the passed in uri and hadoop configuration and set options.
       <p>
       <b>WARNING</b> Note that, since this is called more than once, it is
       critical to ensure that we do not change or reset anything we've already set.
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        elasticSearchSetup(location, job);
    }

    /**
       Given a URI query string, eg. "foo=bar&happy=true" returns
       a hashmap ({'foo' => 'bar', 'happy' => 'true'})
     */
    private HashMap<String, String> parseURIQuery(String query) {
        HashMap<String, String> argMap = new HashMap<String, String>();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] splitPair = pair.split("=");
                argMap.put(splitPair[0], splitPair[1]);
            }
        }
        return argMap;
    }

    /**
	 * Recursively converts an arbitrary object into the appropriate writable.
	 * Please enlighten me if there is an existing method for doing this.
	 */
	private MapWritable tupleToWritable(Tuple tuple, String[] fieldNames) {
		List<Object> attributes = tuple.getAll();
		MapWritable listOfThings = new MapWritable();
		for (int i = 0; i < fieldNames.length; i++) {
			listOfThings.put(toWritable(fieldNames[i]),
					toWritable(((List) attributes).get(i)));
		}
		return listOfThings;

	}

	/**
	 * Recursively converts an arbitrary object into the appropriate writable.
	 * Please enlighten me if there is an existing method for doing this.
    */
    private Writable toWritable(Object thing) {
        if (thing instanceof String) {
            return new Text((String)thing);
        } else if (thing instanceof Long) {
            return new LongWritable((Long)thing);
        } else if (thing instanceof Integer) {
            return new IntWritable((Integer)thing);
        } else if (thing instanceof Double) {
            return new DoubleWritable((Double)thing);
        } else if (thing instanceof Float) {
            return new FloatWritable((Float)thing);
        } else if (thing instanceof Boolean) {
            return new BooleanWritable((Boolean)thing);
        } else if (thing instanceof Map) {
            MapWritable result = new MapWritable();
            for (Map.Entry<String,Object> entry : ((Map<String,Object>)thing).entrySet()) {
                result.put(new Text(entry.getKey().toString()), toWritable(entry.getValue()));
            }
            return result;
        } else if (thing instanceof List) {
            if (((List)thing).size() > 0) {
                Object first = ((List)thing).get(0);
                Writable[] listOfThings = new Writable[((List)thing).size()];
                for (int i = 0; i < listOfThings.length; i++) {
                    listOfThings[i] = toWritable(((List)thing).get(i));
                }
                return new ArrayWritable(toWritable(first).getClass(), listOfThings);
            }
        }
        return NullWritable.get();
    }
    
    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
    }

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
	}
}
