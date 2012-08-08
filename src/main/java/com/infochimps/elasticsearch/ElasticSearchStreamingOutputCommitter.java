package com.infochimps.elasticsearch;

import java.io.IOException;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class ElasticSearchStreamingOutputCommitter extends OutputCommitter {
    
    @Override
    public void setupJob(JobContext context) throws IOException {
      
    }
    
    @Override
    public void cleanupJob(JobContext context) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
	return false;
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
    }
    
    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
    }
    
}
