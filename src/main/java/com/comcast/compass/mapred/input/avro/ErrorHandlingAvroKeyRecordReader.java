package com.comcast.compass.mapred.input.avro;

import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroRecordReaderBase;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
 * This class is an extension of the {@link AvroRecordReaderBase} class. The main purpose of this class is to catch exceptions that are a result of corrupt Avro records.
 * Without this class Spark would typically fail on the spot once it comes across an Avro record that is not valid.
 *
 * An additional feature of this class is that it ignores files that contain the .tmp suffix. Typically, files containing .tmp suffix in HDFS are being written to.
 *
 * @author Leemay Nassery
 */
public class ErrorHandlingAvroKeyRecordReader<T>  extends AvroRecordReaderBase<AvroKey<T>, NullWritable, T> {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlingAvroKeyRecordReader.class);
    private final AvroKey<T> mCurrentRecord = new AvroKey((Object)null);
    private final String TEMP_FILE_SUFFIX = ".tmp";

    public ErrorHandlingAvroKeyRecordReader(Schema readerSchema) {
        super(readerSchema);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        try {
            boolean hasNext = super.nextKeyValue();
            if (!hasNext){
                return false;
            }

            this.mCurrentRecord.datum(this.getCurrentRecord());
            return true;
        } catch (ArrayIndexOutOfBoundsException|AvroRuntimeException ex) {
            LOG.error("Exception that's most likely a result of a malformed Avro record: {}", ex, ex);
            mCurrentRecord.datum(null);
            return false;
        } catch (Exception ex) {
            LOG.error("Exception occurred while consuming Avro record: {}", ex, ex);
            return false;
        }
    }

    public AvroKey<T> getCurrentKey() throws IOException, InterruptedException {
        return this.mCurrentRecord;
    }

    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)inputSplit;
        if(fileSplit != null && fileSplit.getPath() != null && fileSplit.getPath().toString().endsWith(TEMP_FILE_SUFFIX)){
            LOG.info("Not processing Avro tmp file {}", fileSplit.getPath());
        }else {
            super.initialize(inputSplit, context);
        }
    }
}
