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

import java.util.UUID;

import org.apache.cassandra.bridge.RowBufferMode;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.jetbrains.annotations.NotNull;

public class CassandraJobInfo implements JobInfo
{
    private static final long serialVersionUID = 6140098484732683759L;
    protected final BulkSparkConf conf;
    protected final UUID restoreJobId;
    protected final TokenPartitioner tokenPartitioner;

    protected CassandraJobInfo(UUID restoreJobId, BulkSparkConf conf, TokenPartitioner tokenPartitioner)
    {
        this.restoreJobId = restoreJobId;
        this.conf = conf;
        this.tokenPartitioner = tokenPartitioner;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return conf.consistencyLevel;
    }

    @Override
    public String getLocalDC()
    {
        return conf.localDC;
    }

    @NotNull
    @Override
    public RowBufferMode getRowBufferMode()
    {
        return conf.rowBufferMode;
    }

    @Override
    public int getSstableDataSizeInMB()
    {
        return conf.sstableDataSizeInMB;
    }

    @Override
    public int getSstableBatchSize()
    {
        return conf.sstableBatchSize;
    }

    @Override
    public int getCommitBatchSize()
    {
        return conf.commitBatchSize;
    }

    @Override
    public boolean skipExtendedVerify()
    {
        return conf.skipExtendedVerify;
    }

    @Override
    public boolean getSkipClean()
    {
        return conf.getSkipClean();
    }

    @Override
    public DataTransportInfo getTransportInfo()
    {
        return conf.getTransportInfo();
    }

    @Override
    public int getCommitThreadsPerInstance()
    {
        return conf.commitThreadsPerInstance;
    }

    @Override
    public UUID getRestoreJobId()
    {
        return restoreJobId;
    }

    @Override
    public String getConfiguredJobId()
    {
        return conf.configuredJobId;
    }

    @Override
    public TokenPartitioner getTokenPartitioner()
    {
        return tokenPartitioner;
    }

    @Override
    @NotNull
    public QualifiedTableName getQualifiedTableName()
    {
        return new QualifiedTableName(conf.keyspace, conf.table);
    }
}
