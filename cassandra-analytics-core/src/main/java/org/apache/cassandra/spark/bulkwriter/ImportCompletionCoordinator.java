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
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.cassandra.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobStreamResult;
import org.apache.cassandra.spark.bulkwriter.blobupload.CreatedRestoreSlice;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;

import static org.apache.cassandra.clients.Sidecar.toSidecarInstance;
import static org.apache.cassandra.spark.bulkwriter.blobupload.CreatedRestoreSlice.ConsistencyLevelCheckResult.NOT_SATISFIED;
import static org.apache.cassandra.spark.bulkwriter.blobupload.CreatedRestoreSlice.ConsistencyLevelCheckResult.SATISFIED;

public final class ImportCompletionCoordinator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCompletionCoordinator.class);
    private final long startTimeNanos;
    private final BulkWriterContext writerContext;
    private final BlobDataTransferApi dataTransferApi;
    private final BulkWriteValidator writeValidator;
    private final List<BlobStreamResult> blobStreamResultList;
    private final JobInfo job;
    private final ReplicationFactor replicationFactor;
    private final StorageTransportExtension extension;
    private final CompletableFuture<Void> firstFailure = new CompletableFuture<>();
    private final List<CompletableFuture<?>> results = new ArrayList<>();

    private ImportCompletionCoordinator(long startTimeNanos,
                                        BulkWriterContext writerContext, BlobDataTransferApi dataTransferApi,
                                        BulkWriteValidator writeValidator, List<BlobStreamResult> blobStreamResultList,
                                        StorageTransportExtension extension)
    {
        this.startTimeNanos = startTimeNanos;
        this.writerContext = writerContext;
        this.job = writerContext.job();
        this.replicationFactor = writeValidator.replicationFactor();
        this.dataTransferApi = dataTransferApi;
        this.writeValidator = writeValidator;
        this.blobStreamResultList = blobStreamResultList;
        this.extension = extension;
    }

    public static ImportCompletionCoordinator of(long startTimeNanos,
                                                 BulkWriterContext writerContext,
                                                 BlobDataTransferApi dataTransferApi,
                                                 BulkWriteValidator writeValidator,
                                                 List<BlobStreamResult> resultsAsBlobStreamResults,
                                                 StorageTransportExtension extension)
    {
        return new ImportCompletionCoordinator(startTimeNanos, writerContext, dataTransferApi, writeValidator, resultsAsBlobStreamResults, extension);
    }

    /**
     * Block for the imports to complete by invoking the CreateRestoreJobSlice call to the server.
     * The method passes when the successful import can satisfy the configured consistency level;
     * otherwise, the method fails.
     * The wait is indefinite until one of the following conditions is met,
     * 1) _all_ slices have been checked, or
     * 2) the spark job reaches to its completion timeout
     * 3) At least one slice fails CL validation, as the job will eventually fail in this case.
     *    this means that some slices may never be processed by this loop
     * <p>
     * When there is a slice failed on CL validation and there are remaining slices to check, the wait continues.
     */
    public void waitForCompletion()
    {
        writeValidator.setPhase("WaitForCommitCompletion");
        BulkSparkConf conf = writerContext.conf();
        for (BlobStreamResult blobStreamResult : blobStreamResultList)
        {
            for (CreatedRestoreSlice createdRestoreSlice : blobStreamResult.createdRestoreSlices)
            {
                for (RingInstance instance : blobStreamResult.passed)
                {
                    CompletableFuture<Void> fut = createSliceInstanceFuture(createdRestoreSlice,
                                                                            instance,
                                                                            conf);
                    results.add(fut);
                }
            }
        }

        // the result either fail early once firstFailure future completes exceptionally, or the results list completes
        CompletableFuture<?> result = CompletableFuture.anyOf(firstFailure, CompletableFuture.allOf(results.toArray(new CompletableFuture[0])));
        result.join();
        // double check to make sure all slices are satisfied
        // Because at this point all ranges have been either satisfied or the job has already failed,
        // this is really just a sanity check for things like lost futures/future-introduced bugs
        validateAllRangesAreSatisfied();
    }

    private CompletableFuture<Void> createSliceInstanceFuture(CreatedRestoreSlice createdRestoreSlice,
                                                              RingInstance instance,
                                                              BulkSparkConf conf)
    {
        if (firstFailure.isCompletedExceptionally())
        {
            return CompletableFuture.completedFuture(null);
        }
        SidecarInstance sidecarInstance = toSidecarInstance(instance, conf);
        CreateSliceRequestPayload createSliceRequestPayload = createdRestoreSlice.sliceRequestPayload();
        CompletableFuture<Void> fut = dataTransferApi.createRestoreSliceFromDriver(sidecarInstance,
                                                                                   createSliceRequestPayload);
        fut = fut.handleAsync((ignored, throwable) -> {
            if (throwable == null)
            {
                handleSuccessfulSliceInstance(createdRestoreSlice, instance, createSliceRequestPayload);
            }
            else
            {
                handleFailedSliceInstance(instance, createSliceRequestPayload, firstFailure, results, throwable, sidecarInstance);
            }
            return null;
        });
        return fut;
    }

    private void handleFailedSliceInstance(RingInstance instance,
                                           CreateSliceRequestPayload createSliceRequestPayload,
                                           CompletableFuture<Void> firstFailure,
                                           List<CompletableFuture<?>> results,
                                           Throwable throwable,
                                           SidecarInstance sidecarInstance)
    {
        Range<BigInteger> range = Range.openClosed(createSliceRequestPayload.startToken(),
                                                   createSliceRequestPayload.endToken());
        LOGGER.error("Failed to import. slice={} instance={}",
                     createSliceRequestPayload, sidecarInstance, throwable);
        writeValidator.updateFailureHandler(range, instance, "Failed to import slice. " + throwable.getMessage());
        // it either passes or throw if consistency level cannot be satisfied
        try
        {
            writeValidator.validateCLOrFail();
        }
        catch (RuntimeException rte)
        {
            // record the first failure and cancel queued futures.
            firstFailure.completeExceptionally(rte);
            results.forEach(f -> f.cancel(true));
        }
    }

    private void handleSuccessfulSliceInstance(CreatedRestoreSlice createdRestoreSlice,
                                               RingInstance instance,
                                               CreateSliceRequestPayload createSliceRequestPayload)
    {
        createdRestoreSlice.addSucceededInstance(instance);
        if (SATISFIED ==
            createdRestoreSlice.checkForConsistencyLevel(job.getConsistencyLevel(),
                                                         replicationFactor,
                                                         job.getLocalDC()))
        {
            try
            {
                extension.onObjectApplied(createSliceRequestPayload.bucket(),
                                          createSliceRequestPayload.key(),
                                          createSliceRequestPayload.compressedSizeOrZero(),
                                          System.nanoTime() - startTimeNanos);
            }
            catch (Throwable t)
            {
                // log a warning message and carry on
                LOGGER.warn("StorageTransportExtension fails to process ObjectApplied notification", t);
            }
        }
    }

    /**
     * Validate that all ranges should collect enough write acknowledges to satisfy the consistency level
     * It throws when there is any range w/o enough write acknowledges
     */
    private void validateAllRangesAreSatisfied()
    {
        List<CreatedRestoreSlice> unsatisfiedSlices = new ArrayList<>();
        for (BlobStreamResult blobStreamResult : blobStreamResultList)
        {
            for (CreatedRestoreSlice createdRestoreSlice : blobStreamResult.createdRestoreSlices)
            {
                if (NOT_SATISFIED == createdRestoreSlice.checkForConsistencyLevel(job.getConsistencyLevel(),
                                                                                  replicationFactor,
                                                                                  job.getLocalDC()))
                {
                    unsatisfiedSlices.add(createdRestoreSlice);
                }
            }
        }
        if (unsatisfiedSlices.isEmpty())
        {
            LOGGER.info("All token ranges have satisfied with consistency level. consistencyLevel={} phase={}",
                        job.getConsistencyLevel(), writeValidator.getPhase());
        }
        else
        {
            String message = String.format("Some of the token ranges cannot satisfy with consistency level. " +
                                           "job=%s phase=%s consistencyLevel=%s ranges=%s",
                                           job.getRestoreJobId(), writeValidator.getPhase(), job.getConsistencyLevel(), unsatisfiedSlices);
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
    }
}
