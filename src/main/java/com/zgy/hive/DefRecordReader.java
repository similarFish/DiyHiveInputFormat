package com.zgy.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class DefRecordReader implements RecordReader<LongWritable, Text> {

    private static final Logger logs = LoggerFactory.getLogger(DefRecordReader.class.getName());
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private final Seekable filePosition;
    //field separator
    private String FieldSep;
    private static final String defaultFSep="\u0001";
    //"US-ASCII""ISO-8859-1""UTF-8""UTF-16BE""UTF-16LE""UTF-16"
    private final static String defaultEncoding = "UTF-8";
    int maxLineLength;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private String encoding = null;

    public DefRecordReader(Configuration job, FileSplit split) throws IOException{
        this(job,split,null);
    }

    public DefRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter) throws IOException{
        this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
                LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
        this.FieldSep = job.get("textinputformat.record.fieldsep",defaultFSep);
        this.encoding = job.get("textinputformat.record.encoding",defaultEncoding);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        if (isCompressedInput()) {
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec)codec).createInputStream(
                                fileIn, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                in = new CompressedSplitLineReader(cIn, job, recordDelimiter);
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                filePosition = cIn; // take pos from compressed stream
            } else {
                if (start != 0) {
                    // So we have a split that is part of a file stored using
                    // a Compression codec that cannot be split.
                    throw new IOException("Cannot seek in " +
                            codec.getClass().getSimpleName() + " compressed stream");
                }

                in = new SplitLineReader(codec.createInputStream(fileIn,
                        decompressor), job, recordDelimiter);
                filePosition = fileIn;
            }
        } else {
            fileIn.seek(start);
            in = new UncompressedSplitLineReader(
                    fileIn, job, recordDelimiter, split.getLength());
            filePosition = fileIn;
        }
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }

    public DefRecordReader(InputStream in, long offset, long endOffset,int maxLineLength) {
        this(in, offset, endOffset, maxLineLength, null);
    }

    public DefRecordReader(InputStream in, long offset, long endOffset,
                               int maxLineLength, byte[] recordDelimiter) {
        this.maxLineLength = maxLineLength;
        this.in = new SplitLineReader(in, recordDelimiter);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        filePosition = null;
    }

    public DefRecordReader(InputStream in, long offset, long endOffset,
                               Configuration job)
            throws IOException {
        this(in, offset, endOffset, job, null);
    }

    public DefRecordReader(InputStream in, long offset, long endOffset,
                               Configuration job, byte[] recordDelimiter)
            throws IOException {
        this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
                LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
        this.in = new SplitLineReader(in, job, recordDelimiter);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        filePosition = null;
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    private int skipUtfByteOrderMark(Text value) throws IOException {
        // Strip BOM(Byte Order Mark)
        // Text only support UTF-8, we only need to check UTF-8 BOM
        // (0xEF,0xBB,0xBF) at the start of the text stream.
        int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
                Integer.MAX_VALUE);
        int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
        // Even we read 3 extra bytes for the first line,
        // we won't alter existing behavior (no backwards incompat issue).
        // Because the newSize is less than maxLineLength and
        // the number of bytes copied to Text is always no more than newSize.
        // If the return size from readLine is not less than maxLineLength,
        // we will discard the current line and read the next line.
        pos += newSize;
        int textLength = value.getLength();
        byte[] textBytes = value.getBytes();
        if ((textLength >= 3) && (textBytes[0] == (byte) 0xEF) &&
                (textBytes[1] == (byte) 0xBB) && (textBytes[2] == (byte) 0xBF)) {
            // find UTF-8 BOM, strip it.
            logs.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                // It may work to use the same buffer and not do the copyBytes
                textBytes = value.copyBytes();
                value.set(textBytes, 3, textLength);
            } else {
                value.clear();
            }
        }
        return newSize;
    }

    /**
     * Read a line.
     */
    @Override
    public synchronized boolean next(LongWritable key, Text value)
            throws IOException {
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
            key.set(pos);

            int newSize = 0;
            if (pos == 0) {
                newSize = skipUtfByteOrderMark(value);
            } else {
                newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
                pos += newSize;
            }

            if (newSize == 0) {
                return false;
            }
            if (encoding.compareTo(defaultEncoding) != 0) {
                String str = new String(value.getBytes(), 0, value.getLength(), encoding);
                value.set(str);
            }
            if (FieldSep.compareTo(defaultFSep) != 0) {
                String replacedValue = value.toString().replace(FieldSep, defaultFSep);
                value.set(replacedValue);
            }
            if (newSize < maxLineLength) {
                return true;
            }

            // line too long. try again
            logs.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }
        return false;
    }

    /**
     * Get the progress within the split
     */
    @Override
    public synchronized float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }

    private int maxBytesToConsume(long pos) {
        return isCompressedInput()
                ? Integer.MAX_VALUE
                : (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
    }

    private boolean isCompressedInput() {
        return (codec != null);
    }

    public static class LineReader extends org.apache.hadoop.util.LineReader{

        public LineReader(InputStream in) {
            super(in);
        }

        public LineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        public LineReader(InputStream in, Configuration conf) throws IOException {
            super(in, conf);
        }

        public LineReader(InputStream in, byte[] recordDelimiterBytes) {
            super(in, recordDelimiterBytes);
        }

        public LineReader(InputStream in, int bufferSize, byte[] recordDelimiterBytes) {
            super(in, bufferSize, recordDelimiterBytes);
        }

        public LineReader(InputStream in, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
            super(in, conf, recordDelimiterBytes);
        }
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }
}
