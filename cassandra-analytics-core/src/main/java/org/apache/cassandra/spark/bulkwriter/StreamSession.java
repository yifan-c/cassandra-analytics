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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;

public abstract class StreamSession<T extends TransportContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSession.class);
    protected final T transportContext;
    protected final String sessionID;
    protected final Range<BigInteger> tokenRange;
    protected final List<RingInstance> replicas;
    protected final List<StreamError> errors = new ArrayList<>();
    protected final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    protected final ExecutorService executor;
    protected final List<Future<?>> futures = new ArrayList<>();
    protected final CassandraRing<RingInstance> ring;
    protected long rowCount = 0; // total number of rows written by the SSTableWriter

    @VisibleForTesting
    protected StreamSession(T transportContext,
                            String sessionID,
                            Range<BigInteger> tokenRange,
                            ExecutorService executor)
    {
        this.transportContext = transportContext;
        this.ring = transportContext.cluster().getRing(true);
        this.failureHandler = new ReplicaAwareFailureHandler<>(ring);
        this.sessionID = sessionID;
        this.tokenRange = tokenRange;
        this.replicas = getReplicas();
        this.executor = executor;
    }

    public void scheduleStream(SortedSSTableWriter sstableWriter, boolean isLast)
    {
        Preconditions.checkState(!sstableWriter.getTokenRange().isEmpty(), "Trying to stream empty SSTable");

        Preconditions.checkState(tokenRange.encloses(sstableWriter.getTokenRange()),
                                 String.format("SSTable range %s should be enclosed in the partition range %s",
                                               sstableWriter.getTokenRange(), tokenRange));

        rowCount += sstableWriter.rowCount();
        doScheduleStream(sstableWriter, isLast);
    }

    protected void closeFutures()
    {
        for (Future future : futures)
        {
            try
            {
                future.get();
            }
            catch (Exception exception)
            {
                LOGGER.error("Unexpected stream errMsg. "
                             + "Stream errors should have converted to StreamError and sent to driver", exception);
                throw new RuntimeException(exception);
            }
        }

        executor.shutdown();
        LOGGER.info("[{}]: Closing stream session. Sent {} batches of SSTables", sessionID, futures.size());
    }

    @VisibleForTesting
    List<RingInstance> getReplicas()
    {
        Map<Range<BigInteger>, List<RingInstance>> overlappingRanges = ring.getSubRanges(tokenRange).asMapOfRanges();

        Preconditions.checkState(overlappingRanges.keySet().size() == 1,
                                 String.format("Partition range %s is mapping more than one range %s",
                                               tokenRange, overlappingRanges));

        List<RingInstance> replicaList = overlappingRanges.values().stream()
                                                          .flatMap(Collection::stream)
                                                          .distinct()
                                                          .collect(Collectors.toList());
        List<RingInstance> availableReplicas = validateReplicas(replicaList);
        // In order to better utilize replicas, shuffle the replicaList so each session starts writing to a different replica first
        Collections.shuffle(availableReplicas);
        return availableReplicas;
    }

    private List<RingInstance> validateReplicas(List<RingInstance> replicaList)
    {
        Map<Boolean, List<RingInstance>> groups = replicaList.stream()
                                                             .collect(Collectors.partitioningBy(transportContext.cluster()::instanceIsAvailable));
        groups.get(false).forEach(instance -> {
            String errorMessage = String.format("Instance %s is not available.", instance.getNodeName());
            failureHandler.addFailure(tokenRange, instance, errorMessage);
            errors.add(new StreamError(tokenRange, instance, errorMessage));
        });
        return groups.get(true);
    }

    /**
     * Schedule the stream on {@link #executor}
     * @param sstableWriter produces SSTable(s)
     * @param isLast indicate whether it is the last flush for the task
     */
    protected abstract void doScheduleStream(SortedSSTableWriter sstableWriter, boolean isLast);

    /**
     * Send the SSTable(s) written by SSTableWriter
     * The code runs on a separate thread
     *
     * @param sstableWriter produces SSTable(s)
     */
    protected abstract void sendSSTables(SortedSSTableWriter sstableWriter);

    /**
     * Close the stream session
     * @return stream result
     * @throws ExecutionException execution exception during streaming
     * @throws InterruptedException thread interruption
     */
    public abstract StreamResult close() throws ExecutionException, InterruptedException;
}
