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

package org.apache.cassandra.spark.bulkwriter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;

/**
 * Stream session for bulk writes to keyspaces with mutation tracking enabled.
 *
 * <p>
 * Cassandra's coordinated transfer: import can be triggered on any one of the token range's write replicas, and that
 * node then streams the data to the rest of the range's replicas and enforces the requested consistency level
 * internally (see {@code TrackedImportTransfer}, which streams to itself before propagating to its peers). This is unlike a direct write, where Analytics
 * uploads and imports on every replica independently, which would cause duplicate row updates for tracked keyspaces
 * once each replica's coordinated transfer kicks in.
 * <p>
 * {@code TrackedDirectStreamSession} therefore uploads and imports to a <em>single</em> replica per token range
 * instead of all of them. It reuses {@link DirectStreamSession}'s upload/cleanup machinery unchanged, and only
 * differs in:
 * <ul>
 *     <li>{@link #getReplicas()} — selects one replica instead of all of them, spreading the pick across the
 *     eligible candidates by range so that different token ranges (and therefore different Spark tasks) prefer
 *     different nodes rather than funneling every range through the same one</li>
 *     <li>{@link #doFinalizeStream()} — validates success against the single chosen replica rather than the
 *     replica-count-based consistency check {@link DirectStreamSession} uses, since Cassandra's coordinated
 *     transfer -- not Analytics -- is responsible for satisfying the consistency level across the rest of the range</li>
 * </ul>
 * <p>
 * Resiliency to coordinator failure mid-session (e.g. immediately retrying a different replica without failing the
 * whole task) is not handled here; a failed upload/import is surfaced as a {@link org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException}
 * and relies on Spark's task-level retry to pick a new session (and therefore a new coordinator candidate, since
 * failed instances are excluded from selection). See the mutation tracking writer support design doc, Phase 2, for
 * further resiliency work (topology changes, in-session failover, etc).
 */
public class TrackedDirectStreamSession extends DirectStreamSession
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TrackedDirectStreamSession.class);
    private static final String WRITE_PHASE = "TrackedUploadAndCommit";

    public TrackedDirectStreamSession(BulkWriterContext writerContext,
                                      SortedSSTableWriter sstableWriter,
                                      TransportContext.DirectDataBulkWriterContext transportContext,
                                      String sessionID,
                                      Range<BigInteger> tokenRange,
                                      ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                      ExecutorService executorService)
    {
        super(writerContext, sstableWriter, transportContext, sessionID, tokenRange, failureHandler, executorService);
    }

    /**
     * Selects a single replica to act as the coordinator for this token range.
     * <p>
     * The pick is deterministic for a given range, but varies from range to range, so that coordinator load is spread
     * across the eligible replicas rather than always landing on e.g. the first replica returned by Sidecar for every
     * range.
     */
    @Override
    List<RingInstance> getReplicas()
    {
        Set<RingInstance> failedInstances = failureHandler.getFailedInstances();
        List<RingInstance> candidates = tokenRangeMapping.getSubRanges(tokenRange)
                                                          .asMapOfRanges().values().stream()
                                                          .flatMap(Collection::stream)
                                                          .distinct()
                                                          .filter(instance -> !failedInstances.contains(instance))
                                                          // stable order so the range-based pick below is reproducible
                                                          .sorted(Comparator.comparing(RingInstance::nodeName))
                                                          .collect(Collectors.toList());

        Preconditions.checkState(!candidates.isEmpty(),
                                 "No eligible coordinator candidates found for range %s", tokenRange);

        RingInstance coordinator = pickCoordinator(candidates);
        LOGGER.info("[{}]: Selected {} as coordinator for tracked range {} out of {} candidates",
                    sessionID, coordinator.nodeName(), tokenRange, candidates.size());
        return Collections.singletonList(coordinator);
    }

    /**
     * Distributes the coordinator pick across candidates using the range's lower endpoint, so token ranges owned by
     * the same replica set don't all pick the same coordinator.
     */
    private RingInstance pickCoordinator(List<RingInstance> candidates)
    {
        int index = tokenRange.lowerEndpoint().mod(BigInteger.valueOf(candidates.size())).intValue();
        return candidates.get(index);
    }

    @Override
    protected StreamResult doFinalizeStream()
    {
        sendRemainingSSTables();
        DirectStreamResult streamResult = new DirectStreamResult(sessionID,
                                                                 tokenRange,
                                                                 errors,
                                                                 new ArrayList<>(replicas),
                                                                 sstableWriter.rowCount(),
                                                                 sstableWriter.bytesWritten());
        List<CommitResult> commitResults;
        try
        {
            commitResults = commit(streamResult);
        }
        catch (Exception exception)
        {
            if (exception instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(exception);
        }
        streamResult.setCommitResults(commitResults);
        LOGGER.debug("[{}]: Tracked StreamResult: {}", sessionID, streamResult);

        // Cassandra's coordinated transfer enforces the requested consistency level internally once the coordinator
        // accepts the import; Analytics only needs to confirm the single coordinator it uploaded/imported to succeeded.
        BulkWriteValidator.validateTrackedKeyspaceCL(tokenRange, errors, commitResults, LOGGER, WRITE_PHASE, writerContext.job());
        return streamResult;
    }
}
