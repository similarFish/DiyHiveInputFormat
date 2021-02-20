package com.zgy.hive;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class DefInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable{
    private CompressionCodecFactory compressionCodecs = null;

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        reporter.setStatus(inputSplit.toString());
        //String delimiter = jobConf.get("textinputformat.record.line.delimiter");
        String delimiter = "\004";
        byte[] recordDelimiterBytes = null;
        if (delimiter != null){
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        return new DefRecordReader(jobConf,(FileSplit) inputSplit,recordDelimiterBytes);
    }

    @Override
    public void configure(JobConf jobConf) {
        compressionCodecs = new CompressionCodecFactory(jobConf);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        final CompressionCodec codec = compressionCodecs.getCodec(filename);
        if (codec == null){
            return true;
        }










































                                                                                                                                                                                                                                                                                                                                          
        return codec instanceof SplittableCompressionCodec;
    }
}
