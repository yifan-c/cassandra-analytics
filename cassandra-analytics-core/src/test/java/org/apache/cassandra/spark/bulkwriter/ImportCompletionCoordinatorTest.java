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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.apple.cassandra.data.CreateSliceRequestPayload;
import com.apple.cassandra.sidecarclient.InternalSidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.request.Request;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobStreamResult;
import org.apache.cassandra.spark.bulkwriter.blobupload.CreatedRestoreSlice;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ImportCompletionCoordinatorTest
{
    BulkWriterContext mockWriterContext;
    BulkWriteValidator writerValidator;
    CassandraRing<RingInstance> ring;
    JobInfo mockJobInfo;
    BulkSparkConf mockConf;
    InternalSidecarClient mockSidecarClient;
    UUID jobId;

    @BeforeEach
    public void setup() throws Exception
    {
        mockSidecarClient = mock(InternalSidecarClient.class);
        mockJobInfo = mock(JobInfo.class);
        jobId = UUID.randomUUID();
        when(mockJobInfo.getRestoreJobId()).thenReturn(jobId);
        when(mockJobInfo.getQualifiedTableName()).thenReturn(new QualifiedTableName("testkeyspace", "testtable"));
        when(mockJobInfo.getConsistencyLevel()).thenReturn(ConsistencyLevel.CL.QUORUM);
        mockConf = mock(BulkSparkConf.class);
        when(mockConf.getSidecarPort()).thenReturn(9043);

        mockWriterContext = mock(BulkWriterContext.class);
        ClusterInfo mockClusterInfo = mock(ClusterInfo.class);
        when(mockWriterContext.cluster()).thenReturn(mockClusterInfo);

        CassandraContext mockCassandraContext = mock(CassandraContext.class);
        when(mockClusterInfo.getCassandraContext()).thenReturn(mockCassandraContext);
        List<RingInstance> allInstances = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
        {
            allInstances.add(ringInstance(i, 10));
        }
        ring = new CassandraRing<>(Partitioner.Murmur3Partitioner,
                                   "testkeyspace",
                                   new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                         ImmutableMap.of("replication_factor", 3)),
                                   allInstances);
        when(mockClusterInfo.getRing(anyBoolean())).thenReturn(ring);
        when(mockCassandraContext.getSidecarClient()).thenReturn(mockSidecarClient);
        when(mockWriterContext.job()).thenReturn(mockJobInfo);
        when(mockWriterContext.conf()).thenReturn(mockConf);
        when(mockConf.getJobKeepAliveMinutes()).thenReturn(-1);

        writerValidator = new BulkWriteValidator(mockWriterContext, ignored -> { });
    }

    @Test
    public void testAwaitForCompletionWithNoErrors()
    {
        List<BlobStreamResult> resultList = buildBlobStreamResult(0, false);
        ImportCompletionCoordinator.of(mockWriterContext, writerValidator, resultList).waitForCompletion();
        validateAllSlicesWereCalled(resultList);
    }

    @Test
    public void testAwaitForCompletionWithNoErrorsAndSlowImport()
    {
        List<BlobStreamResult> resultList = buildBlobStreamResult(0, true);
        ImportCompletionCoordinator.of(mockWriterContext, writerValidator, resultList).waitForCompletion();
        validateAllSlicesWereCalled(resultList);
    }

    @Test // the test scenario has error when checking, but CL passes overall and the import is successful
    public void testAwaitForCompletionWithErrorsAndCLPasses()
    {
        // There is 1 failure in each replica set. 2 out of 3 replicas succeeds.
        List<BlobStreamResult> resultList = buildBlobStreamResult(1, false);
        ImportCompletionCoordinator.of(mockWriterContext, writerValidator, resultList).waitForCompletion();
        validateAllSlicesWereCalled(resultList);
    }

    @Test // the test scenario has errors that fails CL, the import fails
    public void testAwaitForCompletionWithErrorsAndCLFails()
    {
        // There is 2 failure in each replica set. Only 1 out of 3 replicas succeeds.
        String errorMessage = "Failed to load 1 ranges with QUORUM for job " + jobId + " in phase WaitForCommitCompletion";
        List<BlobStreamResult> resultList = buildBlobStreamResult(2, false);
        try
        {
            ImportCompletionCoordinator.of(mockWriterContext, writerValidator, resultList).waitForCompletion();
        }
        catch (RuntimeException exception)
        {
            assertNotNull(exception.getMessage());
            assertTrue(exception.getMessage().contains(errorMessage));
            assertNotNull(exception.getCause());
        }
        validateAllSlicesWereCalled(resultList);
    }

    private List<BlobStreamResult> buildBlobStreamResult(int failedInstanceCount, boolean simulateSlowImport)
    {
        List<BlobStreamResult> resultList = new ArrayList<>();
        int totalInstances = 10;

        for (int i = 0; i < totalInstances; i++)
        {
            List<RingInstance> replicaSet = Arrays.asList(ringInstance(i, totalInstances),
                                                          ringInstance(i + 1, totalInstances),
                                                          ringInstance(i + 2, totalInstances));
            Set<CreatedRestoreSlice> createdRestoreSlices = new HashSet<>();
            int failedPerReplica = failedInstanceCount;
            CreateSliceRequestPayload mockCreateSliceRequestPayload = mock(CreateSliceRequestPayload.class);
            when(mockCreateSliceRequestPayload.startToken()).thenReturn(BigInteger.valueOf(100 * i));
            when(mockCreateSliceRequestPayload.endToken()).thenReturn(BigInteger.valueOf(100 * (1 + i)));
            when(mockCreateSliceRequestPayload.sliceId()).thenReturn(UUID.randomUUID().toString());
            for (RingInstance instance : replicaSet)
            {
                createdRestoreSlices.add(new CreatedRestoreSlice(mockCreateSliceRequestPayload));
                if (simulateSlowImport && i == totalInstances - 1)
                {
                    // only add slowness for the last import
                    when(mockSidecarClient.createRestoreJobSlice(eq(new SidecarInstanceImpl(instance.getNodeName(), 9043)),
                                                                 eq("testkeyspace"),
                                                                 eq("testtable"),
                                                                 eq(jobId),
                                                                 eq(mockCreateSliceRequestPayload)))
                    .thenAnswer((Answer<CompletableFuture<Void>>) invocation -> {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(2000));
                        return CompletableFuture.completedFuture(null);
                    });
                }
                else if (failedPerReplica-- > 0)
                {
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    future.completeExceptionally(RetriesExhaustedException.of(10, mock(Request.class), null));
                    when(mockSidecarClient.createRestoreJobSlice(eq(new SidecarInstanceImpl(instance.getNodeName(), 9043)),
                                                                 eq("testkeyspace"),
                                                                 eq("testtable"),
                                                                 eq(jobId),
                                                                 eq(mockCreateSliceRequestPayload)))
                    .thenReturn(future);
                }
                else
                {
                    when(mockSidecarClient.createRestoreJobSlice(eq(new SidecarInstanceImpl(instance.getNodeName(), 9043)),
                                                                 eq("testkeyspace"),
                                                                 eq("testtable"),
                                                                 eq(jobId),
                                                                 eq(mockCreateSliceRequestPayload)))
                    .thenReturn(CompletableFuture.completedFuture(null));
                }
            }
            BlobStreamResult result = new BlobStreamResult("", mock(Range.class), Collections.emptyList(),
                                                           replicaSet, 0, createdRestoreSlices);
            resultList.add(result);
        }
        return resultList;
    }

    private void validateAllSlicesWereCalled(List<BlobStreamResult> resultList)
    {
        for (BlobStreamResult blobStreamResult : resultList)
        {
            for (RingInstance instance : blobStreamResult.passed)
            {
                for (CreatedRestoreSlice createdRestoreSlice : blobStreamResult.createdRestoreSlices)
                {
                    verify(mockSidecarClient, times(1))
                    .createRestoreJobSlice(eq(new SidecarInstanceImpl(instance.getNodeName(), 9043)),
                                           eq("testkeyspace"),
                                           eq("testtable"),
                                           eq(jobId),
                                           eq(createdRestoreSlice.sliceRequestPayload()));
                }
            }
        }
    }

    private RingInstance ringInstance(int i, int totalInstances)
    {
        int instanceInRing = i % totalInstances + 1;
        return new RingInstance(new RingEntry.Builder()
                                .token(String.valueOf(i * 100))
                                .fqdn("instance-" + instanceInRing)
                                .build());
    }
}
