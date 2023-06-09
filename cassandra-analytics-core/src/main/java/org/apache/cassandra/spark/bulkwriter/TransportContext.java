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

import java.io.Serializable;

import org.apache.cassandra.spark.bulkwriter.blobupload.BlobDataTransferApi;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;

/**
 * An interface that defines the transport context required to perform the bulk writes
 */
public interface TransportContext
{
    /**
     * @return Spark configuration for bulk writer job
     */
    BulkSparkConf conf();

    /**
     * @return job information
     */
    JobInfo job();

    /**
     * @return cassandra cluster information
     */
    ClusterInfo cluster();

    /**
     * Create a new stream session that writes data to Cassandra
     * @param taskContext task context to create stream session
     * @return a new stream session
     */
    StreamSession<? extends TransportContext> createStreamSession(TaskContext taskContext);

    /**
     * Context used when prepared SSTables are directly written to C* through Sidecar
     */
    interface DirectDataBulkWriterContext extends TransportContext
    {
        /**
         * @return data transfer API client for the direct write mode
         */
        DirectDataTransferApi dataTransferApi();
    }

    /**
     * Context used when SSTables are uploaded to cloud
     */
    interface CloudStorageTransportContext extends TransportContext
    {
        /**
         * @return data transfer API client for the S3_COMPAT mode
         * Implementation note: never return null result
         */
        BlobDataTransferApi dataTransferApi();

        /**
         * @return configuration for S3_COMPAT
         * Implementation note: never return null result
         */
        @NotNull
        StorageTransportConfiguration transportConfiguration();

        /**
         * @return transport extension instance for S3_COMPAT
         * Implementation note: never return null result
         */
        @NotNull
        StorageTransportExtension transportExtensionImplementation();
    }

    interface TransportContextProvider extends Serializable
    {
        /**
         * Create a new transport context instance
         * @param conf bulk writer spark configuration
         * @param jobInfo spark job information
         * @param clusterInfo cassandra cluster information
         * @param isOnDriver indicates whether the role of the runtime is Spark driver or executor
         * @return a new transport context instance
         */
        TransportContext createContext(@NotNull BulkSparkConf conf,
                                       @NotNull JobInfo jobInfo,
                                       @NotNull ClusterInfo clusterInfo,
                                       boolean isOnDriver);
    }
}
