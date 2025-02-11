/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.analytics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.TTLOption;
import org.apache.cassandra.spark.bulkwriter.TimestampOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * Example showcasing the Cassandra Spark Analytics write and read capabilities
 * <p>
 * Prepare your environment by creating the following keyspace and table
 * <p>
 * Schema for the {@code keyspace}:
 * <pre>
 *     CREATE KEYSPACE spark_test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}
 *     AND durable_writes = true;
 * </pre>
 * <p>
 * Schema for the {@code table}:
 * <pre>
 *     CREATE TABLE spark_test.test (
 *         id BIGINT PRIMARY KEY,
 *         course BLOB,
 *         marks BIGINT
 *     );
 * </pre>
 */
public final class IntegrationTestJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestJob.class);
    private static int sidecarPort;

    private IntegrationTestJob()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            sidecarPort = 9043;
        }
        else
        {
            sidecarPort = Integer.parseInt(args[0]);
        }
        LOGGER.info("Starting Spark job with args={}", Arrays.toString(args));

        SparkConf sparkConf = new SparkConf().setAppName("Sample Spark Cassandra Bulk Reader Job")
                                             .set("spark.master", "local[8]");

        // Add SBW-specific settings
        // TODO: Simplify setting up spark conf
        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);

        SparkSession spark = SparkSession
                             .builder()
                             .config(sparkConf)
                             .getOrCreate();
        SparkContext sc = spark.sparkContext();
        SQLContext sql = spark.sqlContext();
        LOGGER.info("Spark Conf: " + sparkConf.toDebugString());
        int rowCount = 10_000;

        try
        {
            Dataset<Row> written = write(rowCount, sql, sc);
            Dataset<Row> read = read(rowCount, sparkConf, sql, sc);
            checkSmallDataFrameEquality(written, read);
            LOGGER.info("Finished Spark job, shutting down...");
            sc.stop();
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Unexpected exception executing Spark job", throwable);
            try
            {
                sc.stop();
            }
            catch (Throwable ignored)
            {
            }
            throw throwable; // rethrow so the exception bubbles up to test usages.
        }
    }

    private static Dataset<Row> write(long rowCount, SQLContext sql, SparkContext sc)
    {
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sc);
        int parallelism = sc.defaultParallelism();
        boolean addTTLColumn = false;
        boolean addTimestampColumn = false;
        JavaRDD<Row> rows = genDataset(javaSparkContext, rowCount, parallelism, addTTLColumn, addTimestampColumn);
        Dataset<Row> df = sql.createDataFrame(rows, getWriteSchema(addTTLColumn, addTimestampColumn));

        DataFrameWriter<Row> dfWriter = df.write()
                                          .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                                          .option("sidecar_instances", "localhost,localhost2,localhost3")
                                          .option("sidecar_port", sidecarPort)
                                          .option("keyspace", "spark_test")
                                          .option("table", "test")
                                          .option("local_dc", "datacenter1")
                                          .option("bulk_writer_cl", "LOCAL_QUORUM")
                                          .option("number_splits", "-1")
                                          // A constant timestamp and TTL can be used by setting the following options.
                                          // .option(WriterOptions.TTL.name(), TTLOption.constant(20))
                                          // .option(WriterOptions.TIMESTAMP.name(), TimestampOption.constant(System.currentTimeMillis() * 1000))
                                          .mode("append");

        List<String> addedColumns = new ArrayList<>();
        if (addTTLColumn)
        {
            addedColumns.add("ttl");
            dfWriter = dfWriter
                       .option(WriterOptions.TTL.name(), TTLOption.perRow("ttl"));
        }

        if (addTimestampColumn)
        {
            addedColumns.add("timestamp");
            dfWriter = dfWriter
                       .option(WriterOptions.TIMESTAMP.name(), TimestampOption.perRow("timestamp"));
        }

        dfWriter.save();

        if (!addedColumns.isEmpty())
        {
            df = df.drop(addedColumns.toArray(new String[0]));
        }

        return df;
    }

    private static Dataset<Row> read(int expectedRowCount, SparkConf sparkConf, SQLContext sql, SparkContext sc)
    {
        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors", sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;

        Dataset<Row> df = sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
                             .option("sidecar_instances", "localhost,localhost2,localhost3")
                             .option("sidecar_port", sidecarPort)
                             .option("keyspace", "spark_test")
                             .option("table", "test")
                             .option("DC", "datacenter1")
                             .option("snapshotName", UUID.randomUUID().toString())
                             .option("createSnapshot", "true")
                             // Shutdown hooks are called after the job ends, and in the case of integration tests
                             // the sidecar is already shut down before this. Since the cluster will be torn
                             // down anyway, the integration job skips clearing snapshots.
                             .option("clearSnapshot", "false")
                             .option("defaultParallelism", sc.defaultParallelism())
                             .option("numCores", numCores)
                             .option("sizing", "default")
                             .load();

        long count = df.count();
        LOGGER.info("Found {} records", count);

        if (count != expectedRowCount)
        {
                throw new RuntimeException(String.format("Expected %d records but found %d records",
                                                         expectedRowCount,
                                                         count));
        }
        return df;
    }

    private static StructType getWriteSchema(boolean addTTLColumn, boolean addTimestampColumn)
    {
        StructType schema = new StructType()
                            .add("id", LongType, false)
                            .add("course", BinaryType, false)
                            .add("marks", LongType, false);
        if (addTTLColumn)
        {
            schema = schema.add("ttl", IntegerType, false);
        }
        if (addTimestampColumn)
        {
            schema = schema.add("timestamp", LongType, false);
        }
        return schema;
    }

    private static void checkSmallDataFrameEquality(Dataset<Row> expected, Dataset<Row> actual)
    {
        if (actual == null)
        {
            throw new NullPointerException("actual dataframe is null");
        }
        if (expected == null)
        {
            throw new NullPointerException("expected dataframe is null");
        }
        // Simulate `actual` having fewer rows, but all match rows in `expected`.
        // The previous implementation would consider these equal
        // actual = actual.limit(1000);
        if (!actual.exceptAll(expected).isEmpty() || !expected.exceptAll(actual).isEmpty())
        {
            throw new IllegalStateException("The content of the dataframes differs");
        }
    }

    private static JavaRDD<Row> genDataset(JavaSparkContext sc, long records, Integer parallelism,
                                           boolean addTTLColumn, boolean addTimestampColumn)
    {
        long recordsPerPartition = records / parallelism;
        long remainder = records - (recordsPerPartition * parallelism);
        List<Integer> seq = IntStream.range(0, parallelism).boxed().collect(Collectors.toList());
        int ttl = 120; // data will not be queryable in two minutes
        long timeStamp = System.currentTimeMillis() * 1000;
        JavaRDD<Row> dataset = sc.parallelize(seq, parallelism).mapPartitionsWithIndex(
        (Function2<Integer, Iterator<Integer>, Iterator<Row>>) (index, integerIterator) -> {
            long firstRecordNumber = index * recordsPerPartition;
            long recordsToGenerate = index.equals(parallelism) ? remainder : recordsPerPartition;
            java.util.Iterator<Row> rows = LongStream.range(0, recordsToGenerate).mapToObj(offset -> {
                long recordNumber = firstRecordNumber + offset;
                String courseNameString = String.valueOf(recordNumber);
                Integer courseNameStringLen = courseNameString.length();
                Integer courseNameMultiplier = 1000 / courseNameStringLen;
                byte[] courseName = dupStringAsBytes(courseNameString, courseNameMultiplier);
                ArrayList<Object> values = new ArrayList<>(Arrays.asList(recordNumber, courseName, recordNumber));
                if (addTTLColumn)
                {
                    values.add(ttl);
                }
                if (addTimestampColumn)
                {
                    values.add(timeStamp);
                }
                return RowFactory.create(values.toArray());
            }).iterator();
            return rows;
        }, false);
        return dataset;
    }

    private static byte[] dupStringAsBytes(String string, Integer times)
    {
        byte[] stringBytes = string.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(stringBytes.length * times);
        for (int time = 0; time < times; time++)
        {
            buffer.put(stringBytes);
        }
        return buffer.array();
    }
}
