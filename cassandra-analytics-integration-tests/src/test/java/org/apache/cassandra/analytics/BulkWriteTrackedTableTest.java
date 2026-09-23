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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.util.concurrent.Uninterruptibles;
import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests bulk writes to a table in a keyspace with mutation tracking enabled, i.e. a keyspace created with
 * {@code replication_type = 'tracked'} (CEP-45).
 *
 * <p>Unlike an untracked write, where every replica receives its own stream, a tracked write uploads and imports on the
 * coordinator only and lets Cassandra's coordinated transfer propagate the data to the remaining replicas. The test
 * therefore validates the write from three angles: the data is readable at {@code LOCAL_QUORUM}, every replica
 * eventually holds every row locally, and the bulk reader reads back exactly what was written.
 */
class BulkWriteTrackedTableTest extends SharedClusterSparkIntegrationTestBase
{
    static final String TRACKED_KEYSPACE = "spark_test_tracked";
    static final QualifiedName TRACKED_TABLE = new QualifiedName(TRACKED_KEYSPACE, "tracked_bulk_write");
    // A tracked write streams to the coordinator only; the remaining replicas are populated asynchronously
    static final long REPLICATION_TIMEOUT_SECONDS = 120;
    static final long REPLICATION_POLL_INTERVAL_MILLIS = 500;

    @Test
    void bulkWriteToTrackedTable()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(dfWrite, TRACKED_TABLE).save();

        // Validate using CQL
        sparkTestUtils.validateWrites(dfWrite.collectAsList(), queryAllData(TRACKED_TABLE));

        // The write only streamed to the coordinator, so wait for Cassandra to propagate the rows to every replica
        awaitAllReplicasHaveRows(TRACKED_TABLE, ROW_COUNT);

        // Read the data back using Bulk Reader and validate that written and read dataframes are the same
        Dataset<Row> read = bulkReaderDataFrame(TRACKED_TABLE).load();
        checkSmallDataFrameEquality(dfWrite, read);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration conf = super.testClusterConfiguration()
                                                .nodesPerDc(3);
        // Preserve the base instance config (e.g. storage_compatibility_mode) and enable mutation tracking, without
        // which CREATE KEYSPACE ... replication_type = 'tracked' is rejected. The journal directory is configured
        // per-instance by the in-jvm dtest InstanceConfig.
        Map<String, Object> instanceConfig = new HashMap<>();
        if (conf.additionalInstanceConfig != null)
        {
            instanceConfig.putAll(conf.additionalInstanceConfig);
        }
        instanceConfig.put("mutation_tracking.enabled", true);
        return conf.additionalInstanceConfig(instanceConfig);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTrackedTestKeyspace(TRACKED_KEYSPACE, DC1_RF3);
        createTestTable(TRACKED_TABLE, CREATE_TEST_TABLE_STATEMENT);
    }

    void createTrackedTestKeyspace(String keyspace, Map<String, Integer> rf)
    {
        cluster.schemaChangeIgnoringStoppedInstances("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                                                     + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', "
                                                     + generateRfString(rf) + " }"
                                                     + " AND replication_type = 'tracked';");
    }

    void awaitAllReplicasHaveRows(QualifiedName table, int expectedRows)
    {
        String query = String.format("SELECT id FROM %s", table);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(REPLICATION_TIMEOUT_SECONDS);
        Map<Integer, Integer> localRowCounts = new HashMap<>();
        while (true)
        {
            localRowCounts.clear();
            for (int i = 1; i <= cluster.size(); i++)
            {
                IInstance instance = cluster.get(i);
                if (instance.isShutdown())
                {
                    continue;
                }
                localRowCounts.put(instance.config().num(), countRows(instance, query));
            }

            if (localRowCounts.values().stream().allMatch(count -> count == expectedRows))
            {
                return;
            }

            if (System.nanoTime() - deadlineNanos >= 0)
            {
                assertThat(localRowCounts.values())
                .describedAs("Every replica of %s is expected to hold %d rows within %d seconds of the tracked write, "
                             + "local row counts per node: %s",
                             table, expectedRows, REPLICATION_TIMEOUT_SECONDS, localRowCounts)
                .allMatch(count -> count == expectedRows);
                return;
            }

            Uninterruptibles.sleepUninterruptibly(REPLICATION_POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * @param instance the node to query
     * @param query    the query to run against the node's local data only
     * @return the number of rows the node returns for {@code query}
     */
    int countRows(IInstance instance, String query)
    {
        SimpleQueryResult result = instance.executeInternalWithResult(query);
        int rowCount = 0;
        while (result.hasNext())
        {
            result.next();
            rowCount++;
        }
        return rowCount;
    }
}
