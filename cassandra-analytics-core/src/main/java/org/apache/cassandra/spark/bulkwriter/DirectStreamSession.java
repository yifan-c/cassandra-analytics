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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.util.ThreadUtil;
import org.apache.cassandra.spark.common.MD5Hash;
import org.apache.cassandra.spark.common.SSTables;

public class DirectStreamSession extends StreamSession<TransportContext.DirectDataBulkWriterContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectStreamSession.class);
    private static final String WRITE_PHASE = "UploadAndCommit";
    private final AtomicInteger nextSSTableIdx = new AtomicInteger(1);
    private final DirectDataTransferApi directDataTransferApi;

    public DirectStreamSession(TransportContext.DirectDataBulkWriterContext transportContext, String sessionID, Range<BigInteger> tokenRange)
    {
        this(transportContext, sessionID, tokenRange, Executors.newSingleThreadExecutor(ThreadUtil.threadFactory("Session=" + sessionID)));
    }

    public DirectStreamSession(TransportContext.DirectDataBulkWriterContext transportContext, String sessionID,
                               Range<BigInteger> tokenRange, ExecutorService executor)
    {
        super(transportContext, sessionID, tokenRange, executor);
        this.directDataTransferApi = transportContext.dataTransferApi();
    }

    @Override
    protected void doScheduleStream(SortedSSTableWriter sstableWriter, boolean isLast)
    {
        futures.add(executor.submit(() -> sendSSTables(sstableWriter)));
    }

    @Override
    protected void sendSSTables(final SortedSSTableWriter ssTableWriter)
    {
        try (DirectoryStream<Path> dataFileStream = Files.newDirectoryStream(ssTableWriter.getOutDir(),
                                                                             "*Data.db"))
        {
            for (Path dataFile : dataFileStream)
            {
                int ssTableIdx = nextSSTableIdx.getAndIncrement();

                LOGGER.info("[{}]: Pushing SSTable {} to replicas {}", sessionID, dataFile,
                            replicas.stream().map(RingInstance::getNodeName).collect(Collectors.joining(",")));
                replicas.removeIf(replica -> !trySendSSTableToReplica(ssTableWriter, dataFile, ssTableIdx, replica));
            }
        }
        catch (IOException exception)
        {
            LOGGER.error("[{}]: Unexpected exception while streaming SSTables {}",
                         sessionID, ssTableWriter.getOutDir());
            cleanAllReplicas();
            throw new RuntimeException(exception);
        }
        finally
        {
            // Clean up SSTable files once the task is complete
            File tempDir = ssTableWriter.getOutDir().toFile();
            LOGGER.info("[{}]: Removing temporary files after stream session from {}", sessionID, tempDir);
            try
            {
                FileUtils.deleteDirectory(tempDir);
            }
            catch (IOException exception)
            {
                LOGGER.warn("[{}]: Failed to delete temporary directory {}", sessionID, tempDir, exception);
            }
        }
    }

    private boolean trySendSSTableToReplica(SortedSSTableWriter ssTableWriter, Path dataFile,
                                            int ssTableIdx, RingInstance replica)
    {
        try
        {
            sendSSTableToReplica(dataFile, ssTableIdx, replica, ssTableWriter.getFileHashes());
            return true;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}]: Failed to stream range {} to instance {}", sessionID, tokenRange,
                         replica.getNodeName(), exception);
            transportContext.cluster().refreshClusterInfo();
            this.failureHandler.addFailure(this.tokenRange, replica, exception.getMessage());
            errors.add(new StreamError(this.tokenRange, replica, exception.getMessage()));
            clean(replica, sessionID);
            return false;
        }
    }

    private void sendSSTableToReplica(Path dataFile, int ssTableIdx, RingInstance instance,
                                      Map<Path, MD5Hash> fileHashes) throws IOException
    {
        try (DirectoryStream<Path> componentFileStream = Files.newDirectoryStream(dataFile.getParent(),
                                                                                  SSTables.getSSTableBaseName(dataFile) + "*"))
        {
            for (Path componentFile : componentFileStream)
            {
                if (!componentFile.getFileName().toString().endsWith("Data.db"))
                {
                    sendSSTableComponent(componentFile, ssTableIdx, instance, fileHashes.get(componentFile));
                }
            }
            sendSSTableComponent(dataFile, ssTableIdx, instance, fileHashes.get(dataFile));
        }
    }

    private void sendSSTableComponent(Path componentFile, int ssTableIdx, RingInstance instance, MD5Hash fileHash)
    throws IOException
    {
        Preconditions.checkNotNull(fileHash, "All files must have a hash. SSTableWriter should have calculated these. This is a bug.");
        long fileSize = Files.size(componentFile);
        LOGGER.info("[{}]: Uploading {} to {}: Size is {}", this.sessionID, componentFile,
                    instance.getNodeName(), fileSize);
        directDataTransferApi.uploadSSTableComponent(componentFile, ssTableIdx, instance, this.sessionID, fileHash);
    }

    @Override
    public StreamResult close() throws ExecutionException, InterruptedException
    {
        closeFutures();
        if (futures.isEmpty())
        {
            return new DirectStreamResult(sessionID, tokenRange, new ArrayList<>(), new ArrayList<>(), rowCount);
        }
        else
        {
            DirectStreamResult streamResult = new DirectStreamResult(sessionID, tokenRange,
                                                                     errors, new ArrayList<>(replicas), rowCount);
            List<CommitResult> cr = commit(streamResult);
            streamResult.setCommitResults(cr);
            LOGGER.debug("StreamResult: {}", streamResult);
            BulkWriteValidator.validateClOrFail(failureHandler, LOGGER, WRITE_PHASE, transportContext.job());
            return streamResult;
        }
    }

    private List<CommitResult> commit(DirectStreamResult streamResult) throws ExecutionException, InterruptedException
    {
        try (CommitCoordinator cc = CommitCoordinator.commit(transportContext, streamResult))
        {
            List<CommitResult> commitResults = cc.get();
            LOGGER.debug("All CommitResults: {}", commitResults);
            commitResults.forEach(cr -> BulkWriteValidator.updateFailureHandler(cr, WRITE_PHASE, failureHandler));
            return commitResults;
        }
    }

    /* Get all replicas and clean temporary state on them */
    private void cleanAllReplicas()
    {
        Set<RingInstance> instances = new HashSet<>(replicas);
        errors.forEach(streamError -> instances.add(streamError.instance));
        instances.forEach(instance -> clean(instance, sessionID));
    }

    private void clean(RingInstance instance, String sessionID)
    {
        if (transportContext.job().getSkipClean())
        {
            LOGGER.info("Skip clean requested - not cleaning SSTable session {} on instance {}",
                        sessionID, instance.getNodeName());
            return;
        }
        String jobID = transportContext.job().getRestoreJobId().toString();
        LOGGER.info("Cleaning SSTable session {} on instance {}", sessionID, instance.getNodeName());
        try
        {
            directDataTransferApi.cleanUploadSession(instance, sessionID, jobID);
        }
        catch (Exception exception)
        {
            LOGGER.warn("Failed to clean SSTables on {} for session {} and ignoring errMsg", instance.getNodeName(),
                        sessionID, exception);
        }
    }
}
