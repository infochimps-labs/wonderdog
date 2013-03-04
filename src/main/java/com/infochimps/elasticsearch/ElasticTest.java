package com.infochimps.elasticsearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


//
// Simple one-hop bulk indexing hadoop job for elasticsearch. It accepts
// tsv documents, creates batch index requests, and sends records directly
// to the elasticsearch data node that's going to actually index them.
//
public class ElasticTest extends Configured implements Tool {

    private final static Log LOG = LogFactory.getLog(ElasticTest.class);
    
    public static class IndexMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {

        private String[] fieldNames;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields    = value.toString().split("\t");
            MapWritable record = new MapWritable();
            for (int i = 0; i < fields.length; i++) {
                if (i < fieldNames.length) {
                    record.put(new Text(fieldNames[i]), new Text(fields[i]));
                }
            }
            context.write(NullWritable.get(), record);
        }

        //
        // Called once at the beginning of the map task. Sets up the indexing job.
        //
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.fieldNames = conf.get("wonderdog.field.names").split(",");
        } 

    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(ElasticTest.class);
        job.setJobName("ElasticTest");
        job.setMapperClass(IndexMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MapWritable.class);
        job.setOutputFormatClass(ElasticSearchOutputFormat.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            System.out.println(args[i]);
            other_args.add(args[i]);
        }
        // Here we need _both_ an input path and an output path.
        // Output stores failed records so they can be re-indexed
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
    
        try {
            job.waitForCompletion(true);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ElasticTest(), args);
        System.exit(res);
    }
}
