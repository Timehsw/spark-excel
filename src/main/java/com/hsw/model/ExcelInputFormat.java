package com.hsw.model;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by HuShiwei on 2016/7/29 0029.
 */
public class ExcelInputFormat extends FileInputFormat{
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        这里默认是系统实现的RecordReader,按行读取,下面我们自定义这个类ExcelRecordReader.
        return new ExcelRecordReader();
    }
}
