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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.vdurmont.semver4j.Semver;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.client.SimpleSidecarInstancesProvider;
import o.a.c.sidecar.client.shaded.common.request.ImportSSTableRequest;
import o.a.c.sidecar.client.shaded.common.response.SSTableImportResponse;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.secrets.SslConfigSecretsProvider;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class SSTableImportTrackedTableTest extends SharedClusterSparkIntegrationTestBase
{
    static final String TRACKED_KEYSPACE = "sstable_import_tracked";
    static final QualifiedName SOURCE_TABLE = new QualifiedName(TRACKED_KEYSPACE, "tracked_import_source");
    static final QualifiedName TARGET_TABLE = new QualifiedName(TRACKED_KEYSPACE, "tracked_import_target");
    static final int ROW_COUNT = 100;
    static final long SIDECAR_REQUEST_TIMEOUT_SECONDS = 120;

    final DigestAlgorithm digestAlgorithm = new XXHash32DigestAlgorithm();
    SidecarClient sidecarClient;

    @Test
    void importsSSTablesFromTrackedTableThroughSidecar() throws Exception
    {
        IInstance coordinator = cluster.getFirstRunningInstance();

        populateSourceTable();
        coordinator.nodetool("flush", TRACKED_KEYSPACE, SOURCE_TABLE.table());
        String snapshotName = "tracked_import_" + System.currentTimeMillis();
        coordinator.nodetool("snapshot", "-t", snapshotName, TRACKED_KEYSPACE);

        List<List<Path>> ssTables = findSnapshotSSTables(coordinator, SOURCE_TABLE, snapshotName);
        assertThat(ssTables)
        .describedAs("Expected to find SSTable components for %s in snapshot %s on node %d",
                     SOURCE_TABLE, snapshotName, coordinator.config().num())
        .isNotEmpty();

        String uploadId = UUID.randomUUID().toString();
        SidecarInstance instance = sidecarInstance(coordinator);
        for (int ssTableIdx = 0; ssTableIdx < ssTables.size(); ssTableIdx++)
        {
            for (Path component : ssTables.get(ssTableIdx))
            {
                uploadComponent(instance, uploadId, component, ssTableIdx);
            }
        }

        ImportSSTableRequest.ImportOptions importOptions = new ImportSSTableRequest.ImportOptions()
                                                           .verifySSTables(true)
                                                           .extendedVerify(true);
        SSTableImportResponse response = sidecarClient.importSSTableRequest(instance,
                                                                           TRACKED_KEYSPACE,
                                                                           TARGET_TABLE.table(),
                                                                           uploadId,
                                                                           importOptions)
                                                      .get(SIDECAR_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(response.success())
        .describedAs("Sidecar is expected to import the SSTables of tracked table %s into %s, response=%s",
                     SOURCE_TABLE, TARGET_TABLE, response)
        .isTrue();

        // Only now is a cluster-wide read meaningful
        assertThat(formatRows(queryAllData(TARGET_TABLE)))
        .describedAs("Every row of the imported SSTables is expected to be readable from %s", TARGET_TABLE)
        .containsExactlyInAnyOrderElementsOf(expectedRows());
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration conf = super.testClusterConfiguration()
                                                .nodesPerDc(3)
                                                // A tracked import hands the SSTables to
                                                // MutationTrackingService.executeTransfers, which streams them to
                                                // every replica of the range over a real internode connection
                                                // (TrackedImportTransfer -> StreamPlan -> NettyStreamingConnectionFactory).
                                                // Streaming does not go through the in-JVM message sink, so without
                                                // NETWORK the instances never bind their storage port and every
                                                // transfer fails with "Connection refused", surfacing at the Sidecar
                                                // as an unrelated NotSerializableException (see StreamSummary.tableId).
                                                .requestFeature(Feature.NETWORK);
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
        cluster.schemaChangeIgnoringStoppedInstances("CREATE KEYSPACE IF NOT EXISTS " + TRACKED_KEYSPACE
                                                     + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', "
                                                     + generateRfString(DC1_RF3) + " }"
                                                     + " AND replication_type = 'tracked';");
        createTestTable(SOURCE_TABLE, CREATE_TEST_TABLE_STATEMENT);
        createTestTable(TARGET_TABLE, CREATE_TEST_TABLE_STATEMENT);
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        sidecarClient = buildSidecarClient();
    }

    @Override
    protected void afterClusterShutdown()
    {
        if (sidecarClient != null)
        {
            try
            {
                sidecarClient.close();
            }
            catch (Exception exception)
            {
                logger.warn("Failed to close the Sidecar client", exception);
            }
        }
        super.afterClusterShutdown();
    }

    SidecarClient buildSidecarClient()
    {
        List<SidecarInstance> instances =
        SparkTestUtils.sidecarInstancesOptionStream(cluster.delegate(), dnsResolver)
                      .map(hostname -> (SidecarInstance) new SidecarInstanceImpl(hostname, server.actualPort()))
                      .collect(Collectors.toList());

        // SslConfig.create returns null when no mTLS options are set, in which case the client talks plain HTTP
        SslConfig sslConfig = SslConfig.create(mtlsTestHelper.mtlOptionMap());
        SecretsProvider secretsProvider = sslConfig != null ? new SslConfigSecretsProvider(sslConfig) : null;
        try
        {
            return Sidecar.from(new SimpleSidecarInstancesProvider(instances),
                                Sidecar.ClientConfig.create(server.actualPort(), server.actualPort()),
                                secretsProvider);
        }
        catch (IOException exception)
        {
            throw new UncheckedIOException("Failed to build the Sidecar client", exception);
        }
    }

    void uploadComponent(SidecarInstance instance, String uploadId, Path component, int ssTableIdx) throws Exception
    {
        sidecarClient.uploadSSTableRequest(instance,
                                          TRACKED_KEYSPACE,
                                          TARGET_TABLE.table(),
                                          uploadId,
                                          componentName(component, ssTableIdx),
                                          digestAlgorithm.calculateFileDigest(component).toSidecarDigest(),
                                          component.toAbsolutePath().toString())
                     .get(SIDECAR_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    String componentName(Path component, int ssTableIdx)
    {
        String[] parts = component.getFileName().toString().split("-");
        assertThat(parts.length)
        .describedAs("Unexpected SSTable component file name: %s", component.getFileName())
        .isGreaterThanOrEqualTo(3);
        parts[parts.length - 3] = Integer.toString(ssTableIdx);
        return String.join("-", parts);
    }

    SidecarInstance sidecarInstance(IInstance instance) throws IOException
    {
        String hostname = dnsResolver.reverseResolve(JMXUtil.getJmxHost(instance.config()));
        return new SidecarInstanceImpl(hostname, server.actualPort());
    }

    List<List<Path>> findSnapshotSSTables(IInstance instance, QualifiedName table, String snapshotName)
    {
        String[] dataDirs = (String[]) instance.config().getParams().get("data_file_directories");
        String tableDirPrefix = table.table() + "-";
        Set<Path> snapshotDirs = new HashSet<>();
        for (String dataDir : dataDirs)
        {
            Path keyspacePath = Paths.get(dataDir, table.keyspace());
            if (!Files.exists(keyspacePath))
            {
                continue;
            }

            try (Stream<Path> walk = Files.walk(keyspacePath))
            {
                walk.filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().equals(snapshotName))
                    .filter(path -> path.getParent() != null
                                    && path.getParent().getFileName().toString().equals("snapshots")
                                    && path.getParent().getParent() != null
                                    && path.getParent().getParent().getFileName().toString().startsWith(tableDirPrefix))
                    .forEach(snapshotDirs::add);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Failed to locate snapshot " + snapshotName + " for " + table, e);
            }
        }

        List<List<Path>> ssTables = new ArrayList<>();
        for (Path snapshotDir : snapshotDirs)
        {
            try (Stream<Path> dataFiles = Files.list(snapshotDir))
            {
                List<String> baseNames = dataFiles.map(path -> path.getFileName().toString())
                                                  .filter(name -> name.endsWith("-Data.db"))
                                                  .map(name -> name.substring(0, name.length() - "Data.db".length()))
                                                  .collect(Collectors.toList());
                for (String baseName : baseNames)
                {
                    try (Stream<Path> siblings = Files.list(snapshotDir))
                    {
                        List<Path> components =
                        siblings.filter(Files::isRegularFile)
                                .filter(path -> path.getFileName().toString().startsWith(baseName))
                                .sorted(Comparator.comparing(path -> path.getFileName().toString().endsWith("-Data.db")))
                                .collect(Collectors.toList());
                        ssTables.add(components);
                    }
                }
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Failed to list SSTable components in " + snapshotDir, e);
            }
        }

        return ssTables;
    }

    void populateSourceTable()
    {
        for (int i = 0; i < ROW_COUNT; i++)
        {
            String query = String.format("INSERT INTO %s (id, course, marks) VALUES (%d, 'course_%d', %d);",
                                         SOURCE_TABLE, i, i, i);
            cluster.getFirstRunningInstance().coordinator().execute(query, ConsistencyLevel.ALL);
        }
    }

    List<String> expectedRows()
    {
        List<String> expected = new ArrayList<>(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++)
        {
            expected.add(String.format("%d:course_%d:%d", i, i, i));
        }
        return expected;
    }

    List<String> formatRows(Object[][] rows)
    {
        // CREATE_TEST_TABLE_STATEMENT declares (id int, course text, marks int) with id as the partition key
        return Stream.of(rows)
                     .map(row -> String.format("%s:%s:%s", row[0], row[1], row[2]))
                     .collect(Collectors.toList());
    }

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
