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

package org.apache.cassandra.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.data.BasicSupplier;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.test.TestSSTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TestDataLayer extends DataLayer
{
    public static final ExecutorService FILE_IO_EXECUTOR =
            Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("test-file-io-%d")
                                                                      .setDaemon(true)
                                                                      .build());

    @NotNull
    CassandraBridge bridge;
    @NotNull
    Collection<Path> dataDbFiles;
    @Nullable
    CqlTable table;
    final String jobId;

    public TestDataLayer(@NotNull CassandraBridge bridge,
                         @NotNull Collection<Path> dataDbFiles,
                         @Nullable CqlTable table)
    {
        this.bridge = bridge;
        this.dataDbFiles = dataDbFiles;
        this.table = table;
        this.jobId = UUID.randomUUID().toString();
    }

    @Override
    public CassandraBridge bridge()
    {
        return bridge;
    }

    @Override
    public int partitionCount()
    {
        return 0;
    }

    @Override
    public CqlTable cqlTable()
    {
        return table;
    }

    @Override
    public boolean isInPartition(int partitionId, BigInteger token, ByteBuffer key)
    {
        return true;
    }

    @Override
    public CommitLogProvider commitLogs(int partitionId)
    {
        throw new UnsupportedOperationException("Test CommitLogProvider not implemented yet");
    }

    @Override
    public TableIdLookup tableIdLookup()
    {
        throw new UnsupportedOperationException("Test TableIdLookup not implemented yet");
    }

    @Override
    protected ExecutorService executorService()
    {
        return FILE_IO_EXECUTOR;
    }

    @Override
    @NotNull
    public SSTablesSupplier sstables(int partitionId,
                                     @Nullable SparkRangeFilter sparkRangeFilter,
                                     @NotNull List<PartitionKeyFilter> partitionKeyFilters)
    {
        return new BasicSupplier(listSSTables().collect(Collectors.toSet()));
    }

    public Stream<SSTable> listSSTables()
    {
        return dataDbFiles.stream().map(TestSSTable::at);
    }

    @Override
    public Partitioner partitioner()
    {
        return Partitioner.Murmur3Partitioner;
    }

    @Override
    public String jobId()
    {
        return jobId;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        // Falling back to JDK serialization
        out.writeObject(version());
        out.writeObject(dataDbFiles);
        bridge.javaSerialize(out, table);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        // Falling back to JDK deserialization
        bridge = CassandraBridgeFactory.get((CassandraVersion) in.readObject());
        dataDbFiles = (Collection<Path>) in.readObject();
        table = bridge.javaDeserialize(in, CqlTable.class);  // Delegate (de-)serialization of version-specific objects to the Cassandra Bridge
    }
}
