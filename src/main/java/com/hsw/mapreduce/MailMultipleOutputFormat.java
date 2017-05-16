package com.hsw.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by HuShiwei on 2016/7/29 0029.
 */
public abstract class MailMultipleOutputFormat<K extends WritableComparable<?>, V extends Writable>
        extends FileOutputFormat<K, V> {
    private MultiRecordWriter writer = null;
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException,
            InterruptedException {
        if (writer == null) {
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));
        }
        return writer;
    }
    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path workPath = null;
        OutputCommitter committer = super.getOutputCommitter(conf);
        if (committer instanceof FileOutputCommitter) {
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = super.getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("Undefined job output-path");
            }
            workPath = outputPath;
        }
        return workPath;
    }
    /**通过key, value, conf来确定输出文件名（含扩展名）*/
    protected abstract String generateFileNameForKeyValue(K key, V value, Configuration conf);

    public class MultiRecordWriter extends RecordWriter<K, V> {
        /**RecordWriter的缓存*/
        private HashMap<String, RecordWriter<K, V>> recordWriters = null;
        private TaskAttemptContext job = null;
        /**输出目录*/
        private Path workPath = null;
        public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
            super();
            this.job = job;
            this.workPath = workPath;
            recordWriters = new HashMap<String, RecordWriter<K, V>>();
        }
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();
            while (values.hasNext()) {
                values.next().close(context);
            }
            this.recordWriters.clear();
        }
        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            //得到输出文件名
            String baseName = generateFileNameForKeyValue(key, value, job.getConfiguration());
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);
            if (rw == null) {
                rw = getBaseRecordWriter(job, baseName);
                this.recordWriters.put(baseName, rw);
            }
            rw.write(key, value);
        }
        // ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}
        private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            String keyValueSeparator = "\t";//key value 分隔符
            RecordWriter<K, V> recordWriter = null;
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
                        GzipCodec.class);
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
                Path file = new Path(workPath, baseName + codec.getDefaultExtension());
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
                recordWriter = new MailRecordWriter<K, V>(new DataOutputStream(codec
                        .createOutputStream(fileOut)), keyValueSeparator);
            } else {
                Path file = new Path(workPath, baseName);
                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);
                recordWriter = new MailRecordWriter<K, V>(fileOut, keyValueSeparator);
            }
            return recordWriter;
        }
    }
}
