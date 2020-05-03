package com.mao.mapreduce.liuliang.enhance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author bigdope
 * @create 2018-09-07
 **/
public class LogEnhanceOutputFormt<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataOutputStream enhanceOs = fileSystem.create(new Path("/liuliang/output/enhanceLog"));
        FSDataOutputStream toCrawlOs = fileSystem.create(new Path("/liuliang/output/toCrawl"));

        return new LongEnhanceRecordWriter(enhanceOs, toCrawlOs);
    }

    public static class LongEnhanceRecordWriter<K, V> extends RecordWriter<K, V> {
        private FSDataOutputStream enhanceOs;
        private FSDataOutputStream toCrawlOs;

        public LongEnhanceRecordWriter(FSDataOutputStream enhanceOs, FSDataOutputStream toCrawlOs) {
            this.enhanceOs = enhanceOs;
            this.toCrawlOs = toCrawlOs;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            if (key.toString().contains("toCrawl")) {
//            if (key.toString().endsWith("toCrawl")) {
                toCrawlOs.write(key.toString().getBytes());
            } else {
                enhanceOs.write(key.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (enhanceOs != null) {
                enhanceOs.close();
            }
            if (toCrawlOs != null) {
                toCrawlOs.close();
            }
        }
    }

}
