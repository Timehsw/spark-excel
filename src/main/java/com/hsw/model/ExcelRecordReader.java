package com.hsw.model;

import com.hsw.excel.ExcelParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by HuShiwei on 2016/7/29 0029.
 */

/**
 * 读excel的电子表格.key是在文件里面的偏移量,value是一行数据
 * 一个字符串包含了所以的列
 */
public class ExcelRecordReader extends RecordReader<LongWritable, Text> {

    /**
     * 声明key的类型
     **/
    private LongWritable key;

    /**
     * 声明value的类型
     **/
    private Text value;

    private InputStream is;
    private String[] strArrayofLines;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // 把split强制转成FileSplit
        FileSplit split = (FileSplit) genericSplit;
        // 用context获取配置信息
        Configuration job = context.getConfiguration();
        // split获取自己所在的路径
        final Path path = split.getPath();
        // split通过配置文件去获得FileSystem
        final FileSystem fileSystem = path.getFileSystem(job);
        // 分布式文件系统去打开这个split,用文件流的方式去读split
        final FSDataInputStream dataInputStream = fileSystem.open(path);
        is = dataInputStream;
        // 调用解析excel的方法,返回解析结果
        final String line = new ExcelParser().parseExcelData(is);
        this.strArrayofLines = line.split("\n");

    }

    /**
     * 通过全局变量来循环调用这个方法，以此获得每一行的key和value
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable(0);
            value = new Text(strArrayofLines[0]);
        } else {
            if (key.get() < (this.strArrayofLines.length - 1)) {
                long pos = (int) key.get();

                key.set(pos + 1);
                value.set(this.strArrayofLines[(int) (pos + 1)]);
                pos++;
            } else {
                return false;
            }
        }
        if (key == null || value == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 调用此方法,获取当前的key值
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 调用此方法,获取当前的value值
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 所有的事情做完以后,应该做的事情,那就是关闭IO流
     */
    @Override
    public void close() throws IOException {
        if (is != null) {
            is.close();
        }

    }
}
