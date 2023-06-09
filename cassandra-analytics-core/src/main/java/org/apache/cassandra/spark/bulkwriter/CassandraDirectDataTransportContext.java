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

import org.apache.cassandra.spark.bulkwriter.util.TaskContextUtils;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;

public class CassandraDirectDataTransportContext implements TransportContext.DirectDataBulkWriterContext
{
    @NotNull
    private final BulkSparkConf conf;
    @NotNull
    private final JobInfo jobInfo;
    @NotNull
    private final ClusterInfo clusterInfo;
    @NotNull
    private final DirectDataTransferApi dataTransferApi;

    public CassandraDirectDataTransportContext(@NotNull BulkSparkConf conf,
                                               @NotNull JobInfo jobInfo,
                                               @NotNull ClusterInfo clusterInfo,
                                               boolean isOnDriver) // DIRECT mode does not need to distinguish driver and executor
    {
        this.conf = conf;
        this.jobInfo = jobInfo;
        this.clusterInfo = clusterInfo;
        this.dataTransferApi = createDirectDataTransferApi();
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
    public DirectStreamSession createStreamSession(TaskContext taskContext)
    {
        return new DirectStreamSession(this,
                                       TaskContextUtils.createStreamId(taskContext),
                                       TaskContextUtils.getTokenRange(taskContext, jobInfo));
    }

    @Override
    public DirectDataTransferApi dataTransferApi()
    {
        return dataTransferApi;
    }

    // only invoke in constructor
    protected DirectDataTransferApi createDirectDataTransferApi()
    {
        return new SidecarDataTransferApi(clusterInfo.getCassandraContext().getSidecarClient(), jobInfo, conf);
    }
}
