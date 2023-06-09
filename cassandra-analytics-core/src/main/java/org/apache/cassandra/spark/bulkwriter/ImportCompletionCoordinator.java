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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.cassandra.data.CreateSliceRequestPayload;
import com.apple.cassandra.sidecarclient.InternalSidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobStreamResult;
import org.apache.cassandra.spark.bulkwriter.blobupload.CreatedRestoreSlice;

import static org.apache.cassandra.clients.Sidecar.toSidecarInstance;

public final class ImportCompletionCoordinator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCompletionCoordinator.class);
    private final BulkWriterContext writerContext;
    private final BulkWriteValidator writeValidator;
    private final List<BlobStreamResult> blobStreamResultList;

    private ImportCompletionCoordinator(BulkWriterContext writerContext, BulkWriteValidator writeValidator, List<BlobStreamResult> blobStreamResultList)
    {
        this.writerContext = writerContext;
        this.writeValidator = writeValidator;
        this.blobStreamResultList = blobStreamResultList;
    }

    public static ImportCompletionCoordinator of(BulkWriterContext writerContext,
                                                 BulkWriteValidator writeValidator,
                                                 List<BlobStreamResult> resultsAsBlobStreamResults)
    {
        return new ImportCompletionCoordinator(writerContext, writeValidator, resultsAsBlobStreamResults);
    }

    /**
     * Block for the imports to complete by invoking the CreateRestoreJobSlice call to the server.
     * The method passes when the successful import can satisfy the configured consistency level;
     * otherwise, the method fails.
     * The wait is indefinite until one of the following conditions is met,
     * 1) _all_ slices have been checked, or
     * 2) the spark job reaches to its completion timeout
     *
     * When there is a slice failed on CL validation and there are remaining slices to check, the wait continues.
     */
    public void waitForCompletion()
    {
        writeValidator.setPhase("WaitForCommitCompletion");
        InternalSidecarClient sidecarClient = (InternalSidecarClient) writerContext.cluster().getCassandraContext().getSidecarClient();
        QualifiedTableName table = writerContext.job().getQualifiedTableName();
        UUID jobId = writerContext.job().getRestoreJobId();
        BulkSparkConf conf = writerContext.conf();
        List<CompletableFuture<?>> results = new ArrayList<>();
        for (BlobStreamResult blobStreamResult : blobStreamResultList)
        {
            for (RingInstance instance : blobStreamResult.passed)
            {
                SidecarInstance sidecarInstance = toSidecarInstance(instance, conf);
                for (CreatedRestoreSlice createdRestoreSlice : blobStreamResult.createdRestoreSlices)
                {
                    CreateSliceRequestPayload createSliceRequestPayload = createdRestoreSlice.sliceRequestPayload();
                    results.add(sidecarClient.createRestoreJobSlice(sidecarInstance,
                                                                    table.keyspace(),
                                                                    table.tableName(),
                                                                    jobId,
                                                                    createSliceRequestPayload)
                                             .exceptionally(throwable -> {
                                                 LOGGER.error("Failed to import. slice={} instance={}",
                                                              createSliceRequestPayload, sidecarInstance, throwable);
                                                 Range<BigInteger> failedRange = Range.openClosed(createSliceRequestPayload.startToken(),
                                                                                                  createSliceRequestPayload.endToken());
                                                 writeValidator.updateFailureHandler(failedRange, instance, "Failed to import slice. " + throwable.getMessage());
                                                 // it either passes or throw if consistency level cannot be satisfied
                                                 writeValidator.validateCLOrFail();
                                                 return null; // completes this future
                                             }));
                }
            }
        }

        CompletableFuture<?> allResultsFuture = CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
                                                                 .whenComplete((v, throwable) -> {
                                                                     if (throwable != null)
                                                                     {
                                                                         throw new RuntimeException("Failed to import", throwable);
                                                                     }
                                                                 });
        allResultsFuture.join();
    }
}
