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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.ScalaFunctions;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.ShutdownHookManager;
import org.jetbrains.annotations.NotNull;

// CHECKSTYLE IGNORE: This class cannot be declared as final, because consumers should be able to extend it
public class CassandraBulkWriterContext implements BulkWriterContext, KryoSerializable
{
    private static final long serialVersionUID = 4992730766398015088L;
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @NotNull
    protected final BulkSparkConf conf;
    protected final JobInfo jobInfo;
    protected final ClusterInfo clusterInfo;
    protected final SchemaInfo schemaInfo;
    protected transient volatile TransportContext transportContext;

    // Static factory to create BulkWriterContext based on the requested Bulk Writer transport strategy
    public static BulkWriterContext fromOptions(@NotNull SparkContext sparkContext,
                                                @NotNull Map<String, String> strOptions,
                                                @NotNull StructType dfSchema)
    {
        Preconditions.checkNotNull(dfSchema);

        BulkSparkConf conf = new BulkSparkConf(sparkContext.getConf(), strOptions);
        CassandraBulkWriterContext bulkWriterContext = new CassandraBulkWriterContext(conf, dfSchema, sparkContext);
        ShutdownHookManager.addShutdownHook(org.apache.spark.util.ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY(),
                                            ScalaFunctions.wrapLambda(bulkWriterContext::shutdown));
        bulkWriterContext.dialHome(sparkContext.version());

        return bulkWriterContext;
    }

    protected CassandraBulkWriterContext(@NotNull BulkSparkConf conf,
                                         @NotNull StructType dfSchema,
                                         SparkContext sparkContext)
    {
        this.conf = conf;
        clusterInfo = initializeClusterInfo(conf);
        clusterInfo.startupValidate();
        CassandraRing<RingInstance> ring = clusterInfo.getRing(true);
        Preconditions.checkArgument(!conf.consistencyLevel.isLocal()
                                    || (conf.localDC != null && ring.getReplicationFactor().getOptions().containsKey(conf.localDC)),
                                    String.format("Keyspace %s is not replicated on datacenter %s",
                                                  conf.keyspace, conf.localDC));


        CassandraBridge bridge = CassandraBridgeFactory.get(clusterInfo.getLowestCassandraVersion());
        UUID jobId = bridge.getTimeUUID();
        jobInfo = initializeJobInfo(conf, jobId, initializeTokenPartitioner(conf, sparkContext, ring));

        transportContext = createTransportContext(true);

        String keyspace = conf.keyspace;
        String table = conf.table;

        String keyspaceSchema = clusterInfo.getKeyspaceSchema(true);

        Partitioner partitioner = clusterInfo.getPartitioner();
        String tableSchema = CqlUtils.extractTableSchema(keyspaceSchema, keyspace, table);
        Set<String> udts = CqlUtils.extractUdts(keyspaceSchema, keyspace);
        ReplicationFactor replicationFactor = CqlUtils.extractReplicationFactor(keyspaceSchema, keyspace);
        int indexCount = CqlUtils.extractIndexCount(keyspaceSchema, keyspace, table);
        CqlTable cqlTable = bridge.buildSchema(tableSchema, keyspace, replicationFactor, partitioner, udts, null, indexCount);

        TableInfoProvider tableInfoProvider = initializeTableInfoProvider(tableSchema, cqlTable);
        schemaInfo = initializeSchemaInfo(conf, dfSchema, tableInfoProvider);
    }

    @Override
    public void shutdown()
    {
        logger.info("Shutting down {}", this);
        synchronized (this)
        {
            if (clusterInfo != null)
            {
                clusterInfo.close();
            }
        }
    }

    /**
     * Use the implementation of the KryoSerializable interface as a detection device to make sure the Spark Bulk
     * Writer's KryoRegistrator is properly in place.
     * <p>
     * If this class is serialized by Kryo, it means we're <b>not</b> set up correctly, and therefore we log and fail.
     * This failure will occur early in the job and be very clear, so users can quickly fix their code and get up and
     * running again, rather than having a random NullPointerException further down the line.
     */
    public static final String KRYO_REGISTRATION_WARNING =
    "Spark Bulk Writer Kryo Registrator (SbwKryoRegistrator) was not registered with Spark - "
    + "please see the README.md file for more details on how to register the Spark Bulk Writer.";

    @Override
    public void write(Kryo kryo, Output output)
    {
        failIfKryoNotRegistered();
    }

    @Override
    public void read(Kryo kryo, Input input)
    {
        failIfKryoNotRegistered();
    }

    private void failIfKryoNotRegistered()
    {
        logger.error(KRYO_REGISTRATION_WARNING);
        throw new RuntimeException(KRYO_REGISTRATION_WARNING);
    }

    protected void dialHome(String sparkVersion)
    {
        logger.info("Dial home. clientConfig={}, sparkVersion={}", conf, sparkVersion);
    }

    @Override
    @NotNull
    public BulkSparkConf conf()
    {
        return conf;
    }

    @Override
    @NotNull
    public ClusterInfo cluster()
    {
        return clusterInfo;
    }

    @Override
    @NotNull
    public JobInfo job()
    {
        return jobInfo;
    }

    @Override
    public SchemaInfo schema()
    {
        return schemaInfo;
    }

    @Override
    public TransportContext transportContext()
    {
        // When running on driver, transportContext is created at the constructor, and it is not null
        if (transportContext != null)
        {
            return transportContext;
        }

        // When running on executor, transportContext is null. Synchronize to avoid multi-instantiation
        synchronized (this)
        {
            if (transportContext == null)
            {
                transportContext = createTransportContext(false);
            }
        }
        return transportContext;
    }

    @NotNull
    protected TransportContext createTransportContext(boolean isOnDriver)
    {
        return conf.getTransportInfo()
                   .getTransport()
                   .createContext(conf, jobInfo, clusterInfo, isOnDriver);
    }

    @NotNull
    protected ClusterInfo initializeClusterInfo(@NotNull BulkSparkConf conf)
    {
        return new CassandraClusterInfo(conf);
    }

    @NotNull
    protected static TokenPartitioner initializeTokenPartitioner(@NotNull BulkSparkConf conf,
                                                                 @NotNull SparkContext sparkContext,
                                                                 @NotNull CassandraRing<RingInstance> ring)
    {
        return new TokenPartitioner(ring, conf.numberSplits, sparkContext.defaultParallelism(), conf.getCores());
    }

    @NotNull
    protected JobInfo initializeJobInfo(@NotNull BulkSparkConf conf,
                                        @NotNull UUID jobId,
                                        @NotNull TokenPartitioner tokenPartitioner)
    {
        return new CassandraJobInfo(jobId, conf, tokenPartitioner);
    }

    @NotNull
    protected TableInfoProvider initializeTableInfoProvider(@NotNull String tableSchema,
                                                            @NotNull CqlTable cqlTable)
    {
        return new CqlTableInfoProvider(tableSchema, cqlTable);
    }

    @NotNull
    protected SchemaInfo initializeSchemaInfo(@NotNull BulkSparkConf conf,
                                              @NotNull StructType dfSchema,
                                              @NotNull TableInfoProvider tableInfoProvider)
    {
        return new CassandraSchemaInfo(new TableSchema(dfSchema,
                                                       tableInfoProvider,
                                                       conf.writeMode,
                                                       conf.getTTLOptions(),
                                                       conf.getTimestampOptions()));
    }
}
