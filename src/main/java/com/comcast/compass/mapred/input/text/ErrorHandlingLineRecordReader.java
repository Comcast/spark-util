package com.comcast.compass.mapred.input.text;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Copyright 2017 Comcast Cable Communications Management, LLC *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at *
 * http://www.apache.org/licenses/LICENSE-2.0 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Overrides the class {@link org.apache.hadoop.mapreduce.lib.input.LineRecordReader} go handle catching exceptions instead of throwing them. This addition is
 * done to prevent Spark jobs from failing when it comes across a corrupt file or record.
 *
 * @author Leemay Nassery
 */
public class ErrorHandlingLineRecordReader extends RecordReader<LongWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlingLineRecordReader.class);
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;

    public ErrorHandlingLineRecordReader() {
    }

    public ErrorHandlingLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context)  {
        try {
            FileSplit split = (FileSplit)genericSplit;
            Configuration job = context.getConfiguration();
            this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
            this.start = split.getStart();
            this.end = this.start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            this.fileIn = fs.open(file);
            CompressionCodec codec = (new CompressionCodecFactory(job)).getCodec(file);
            if(null != codec) {
                this.isCompressedInput = true;
                this.decompressor = CodecPool.getDecompressor(codec);
                if(codec instanceof SplittableCompressionCodec) {
                    SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                    this.in = new CompressedSplitLineReader(cIn, job, this.recordDelimiterBytes);
                    this.start = cIn.getAdjustedStart();
                    this.end = cIn.getAdjustedEnd();
                    this.filePosition = cIn;
                } else {
                    this.in = new SplitLineReader(codec.createInputStream(this.fileIn, this.decompressor), job, this.recordDelimiterBytes);
                    this.filePosition = this.fileIn;
                }
            } else {
                this.fileIn.seek(this.start);
                this.in = new SplitLineReader(this.fileIn, job, this.recordDelimiterBytes);
                this.filePosition = this.fileIn;
            }

            if(this.start != 0L) {
                this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
            }

            this.pos = this.start;
        }catch(Exception ex){
            LOG.warn("Exception occurred during initialization {}", ex, ex);
        }

    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput?2147483647:(int)Math.max(Math.min(2147483647L, this.end - pos), (long)this.maxLineLength);
    }

    private long getFilePosition()  throws IOException {
        long retVal;
        if(this.isCompressedInput && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }
        return retVal;
    }

    private int skipUtfByteOrderMark() throws IOException {
        int newMaxLineLength = (int) Math.min(3L + (long) this.maxLineLength, 2147483647L);
        int newSize = this.in.readLine(this.value, newMaxLineLength, this.maxBytesToConsume(this.pos));
        this.pos += (long) newSize;
        int textLength = this.value.getLength();
        byte[] textBytes = this.value.getBytes();
        if (textLength >= 3 && textBytes[0] == -17 && textBytes[1] == -69 && textBytes[2] == -65) {
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                textBytes = this.value.copyBytes();
                this.value.set(textBytes, 3, textLength);
            } else {
                this.value.clear();
            }
        }
        return newSize;
    }

    public boolean nextKeyValue() {
        int newSize = 0;

        try {
            if(this.key == null) {
                this.key = new LongWritable();
            }

            this.key.set(this.pos);
            if(this.value == null) {
                this.value = new Text();
            }

            while(this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
                if(this.pos == 0L) {
                    newSize = this.skipUtfByteOrderMark();
                } else {
                    newSize = this.in.readLine(this.value, this.maxLineLength, this.maxBytesToConsume(this.pos));
                    this.pos += (long)newSize;
                }

                if(newSize == 0 || newSize < this.maxLineLength) {
                    break;
                }

                LOG.info("Skipped line of size {} at position {} ", newSize, (this.pos - (long) newSize));
            }

        } catch (Exception ex) {
            LOG.warn("Exception occurred while processing next key value in text file (setting newsize to zero): {}", ex, ex);
            newSize = 0;
        }


        if(newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }

    }

    public LongWritable getCurrentKey() {
        return this.key;
    }

    public Text getCurrentValue() {
        return this.value;
    }

    public float getProgress() throws IOException {
        return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
    }

    public synchronized void close() {
        try {
            if(this.in != null) {
                this.in.close();
            }
        }catch(Exception ex){
            LOG.warn("Exception on close: {}", ex, ex);
        } finally {
            if(this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
            }

        }
    }
}

