package com.comcast.compass.mapred.input.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapreduce.InputSplit;

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
 *
 * Implementation of {@link AvroKeyInputFormat} that uses a custom implementation of the AvroKeyRecordReader that catches exceptions instead of throwing them.
 * This is done to prevent the Spark job from failing due to a corrupt event.
 *
 * Example below of invoking this class for the Spark newAPIHadoopFile API:
 *
 * sparkContext.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable,AtlasAvroKeyInputFormat[GenericRecord]](path_here)
 *
 * @author Leemay Nassery
 */
public class AtlasAvroKeyInputFormat<T> extends AvroKeyInputFormat<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAvroKeyInputFormat.class);

    @Override
    public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Schema readerSchema = AvroJob.getInputKeySchema(taskAttemptContext.getConfiguration());
        if(null == readerSchema) {
            LOG.warn("Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
        }
        return new ErrorHandlingAvroKeyRecordReader(readerSchema);
    }
}
