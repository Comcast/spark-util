# spark-util

A simple helper library that allows your Spark job to handle exceptions related to malformed file formats more gracefully.

Sometimes, Spark can be unforgiving when it comes to consuming files that may contain malformed records. This library contains classes
that extend the Hadoop Avro and text input file formats that are commonly used when consuming files from HDFS via Spark.


Examples
=====================================

An example when consuming Avro files via the Spark framework:

```
 sparkContext.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable,AtlasAvroKeyInputFormat[GenericRecord]]("hdfs://nameservice1/user/data/")

```


An example consuming files containing text records via the Spark framework:

```

 sparkContext.newAPIHadoopFile[LongWritable, Text, AtlasTextInputFileFormat]("hdfs://nameservice1/user/data/")

```