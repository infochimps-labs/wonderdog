package com.infochimps.hadoop.util;

import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;

public class HadoopUtils {
    
    /**
       Upload a local file to the cluster
     */
    public static void uploadLocalFile(Path localsrc, Path hdfsdest, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(false, true, localsrc, hdfsdest);
    }

    
    /**
       Upload a local file to the cluster, if it's newer or nonexistent
     */
    public static void uploadLocalFileIfChanged(Path localsrc, Path hdfsdest, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus l_stat = fs.getFileStatus(localsrc);
        try {
            FileStatus h_stat = fs.getFileStatus(hdfsdest);
            if ( l_stat.getModificationTime() > h_stat.getModificationTime() ) {
                uploadLocalFile(localsrc, hdfsdest, conf);
            }
        }
        catch (FileNotFoundException e) {
            uploadLocalFile(localsrc, hdfsdest, conf);
        }
    }


    /**
       Fetches a file with the basename specified from the distributed cache. Returns null if no file is found
     */
    public static String fetchFromCache(String basename, Configuration conf) throws IOException {
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (Path cacheFile : cacheFiles) {
                if (cacheFile.getName().equals(basename)) {
                    return cacheFile.toString();
                }
            }
        }
        return null;
    }

    /**
       Takes a path on the hdfs and ships it in the distributed cache if it is not already in the distributed cache
     */
    public static void shipIfNotShipped(Path hdfsPath, Configuration conf) throws IOException {
        if (fetchFromCache(hdfsPath.getName(), conf) == null) {
            try {
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }        
    }
}
