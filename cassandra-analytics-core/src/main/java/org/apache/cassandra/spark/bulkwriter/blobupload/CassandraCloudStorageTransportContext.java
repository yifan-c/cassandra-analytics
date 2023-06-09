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

package org.apache.cassandra.spark.bulkwriter.blobupload;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.ClusterInfo;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.TransportContext;
import org.apache.cassandra.spark.bulkwriter.util.TaskContextUtils;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;

public class CassandraCloudStorageTransportContext implements TransportContext.CloudStorageTransportContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCloudStorageTransportContext.class);

    @NotNull
    private final StorageTransportExtension storageTransportExtension;
    @NotNull
    private final StorageTransportConfiguration storageTransportConfiguration;
    @NotNull
    private final BlobDataTransferApi dataTransferApi;
    @NotNull
    private final BulkSparkConf conf;
    @NotNull
    private final JobInfo jobInfo;
    @NotNull
    private final ClusterInfo clusterInfo;

    public CassandraCloudStorageTransportContext(@NotNull BulkSparkConf conf,
                                                 @NotNull JobInfo jobInfo,
                                                 @NotNull ClusterInfo clusterInfo,
                                                 boolean isOnDriver)
    {
        // we may not always need a transport extension implementation in cloud based transport context, revisit this
        // check when we have multiple cloud based transport options supported
        Objects.requireNonNull(conf.getTransportInfo().getTransportExtensionClass(),
                               "DATA_TRANSPORT_EXTENSION_CLASS must be provided");
        this.conf = conf;
        this.jobInfo = jobInfo;
        this.clusterInfo = clusterInfo;
        this.storageTransportExtension = createStorageTransportExtension(isOnDriver);
        this.storageTransportConfiguration = storageTransportExtension.getStorageConfiguration();
        Objects.requireNonNull(storageTransportConfiguration,
                               "Storage configuration cannot be null in order to upload to cloud");
        this.dataTransferApi = createBlobDataTransferApi();
    }

    @Override
    public BulkSparkConf conf()
    {
        return conf;
    }

    @Override
    public JobInfo job()
    {
        return jobInfo;
    }

    @Override
    public ClusterInfo cluster()
    {
        return clusterInfo;
    }

    @Override
    public BlobStreamSession createStreamSession(TaskContext taskContext)
    {
        Path jobBasePath = Paths.get(System.getProperty("java.io.tmpdir"),
                                     jobInfo.getRestoreJobId().toString());
        String streamId = TaskContextUtils.createStreamId(taskContext);
        return new BlobStreamSession(taskContext.partitionId(), taskContext.taskAttemptId(), this,
                                     streamId,
                                     TaskContextUtils.getTokenRange(taskContext, job()),
                                     TaskContextUtils.getPartitionUniquePath(taskContext, streamId, jobBasePath));
    }

    @Override
    public BlobDataTransferApi dataTransferApi()
    {
        return dataTransferApi;
    }

    @NotNull
    @Override
    public StorageTransportConfiguration transportConfiguration()
    {
        return storageTransportConfiguration;
    }

    /**
     * Instantiate and initialize the StorageTransportExtension instance, for only once.
     * @return StorageTransportExtension instance
     */
    @NotNull
    @Override
    public StorageTransportExtension transportExtensionImplementation()
    {
        return this.storageTransportExtension;
    }

    // only invoke it in constructor
    protected BlobDataTransferApi createBlobDataTransferApi()
    {
        StorageClient storageClient = new StorageClient(storageTransportConfiguration,
                                                        conf.getStorageClientConfig());
        return new BlobDataTransferApi(jobInfo,
                                       clusterInfo.getCassandraContext().getSidecarClient(),
                                       storageClient);
    }

    // only invoke it in constructor
    protected StorageTransportExtension createStorageTransportExtension(boolean isOnDriver)
    {
        String transportExtensionClass = conf.getTransportInfo().getTransportExtensionClass();
        try
        {
            Class<StorageTransportExtension> clazz = (Class<StorageTransportExtension>) Class.forName(transportExtensionClass);
            StorageTransportExtension extension = clazz.getConstructor().newInstance();
            LOGGER.info("Initializing storage transport extension. jobId={}, restoreJobId={}",
                        jobInfo.getId(), jobInfo.getRestoreJobId());
            extension.initialize(jobInfo.getId(), conf.getSparkConf(), isOnDriver);
            // Only assign when initialization is complete
            // to avoid exposing uninitialized extension object, which leads to unexpected behavior
            return extension;
        }
        catch (ClassNotFoundException | ClassCastException | InvocationTargetException | InstantiationException
               | IllegalAccessException | NoSuchMethodException e)
        {
            throw new RuntimeException("Invalid storage transport extension class specified: '" + transportExtensionClass, e);
        }
    }
}
