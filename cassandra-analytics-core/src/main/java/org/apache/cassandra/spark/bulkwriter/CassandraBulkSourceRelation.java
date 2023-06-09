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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.cassandra.data.CreateRestoreJobRequestPayload;
import com.apple.cassandra.data.RestoreJobSecrets;
import com.apple.cassandra.data.RestoreJobStatus;
import com.apple.cassandra.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.spark.bulkwriter.blobupload.BlobStreamResult;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportHandler;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.util.control.NonFatal$;

public class CassandraBulkSourceRelation extends BaseRelation implements InsertableRelation
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraBulkSourceRelation.class);

    private final BulkWriterContext writerContext;
    private final SQLContext sqlContext;
    private final JavaSparkContext sparkContext;
    private final Broadcast<BulkWriterContext> broadcastContext;
    private final BulkWriteValidator writeValidator;
    private HeartbeatReporter heartbeatReporter;
    private long startTimeNanos;

    @SuppressWarnings("RedundantTypeArguments")
    public CassandraBulkSourceRelation(BulkWriterContext writerContext, SQLContext sqlContext) throws Exception
    {
        this.writerContext = writerContext;
        this.sqlContext = sqlContext;
        this.sparkContext = JavaSparkContext.fromSparkContext(sqlContext.sparkContext());
        this.broadcastContext = sparkContext.<BulkWriterContext>broadcast(writerContext);
        this.writeValidator = new BulkWriteValidator(writerContext, this::cancelJob);
        onCloudStorageTransport(ignored -> this.heartbeatReporter = new HeartbeatReporter());
    }

    @Override
    @NotNull
    public SQLContext sqlContext()
    {
        return sqlContext;
    }

    /**
     * @return An empty {@link StructType}, as this is a writer only, so schema is not applicable
     */
    @Override
    @NotNull
    public StructType schema()
    {
        LOGGER.warn("This instance is used as writer, a schema is not supported");
        return new StructType();
    }

    /**
     * @return {@code 0} size as not applicable use by the planner in the writer-only use case
     */
    @Override
    public long sizeInBytes()
    {
        LOGGER.warn("This instance is used as writer, sizeInBytes is not supported");
        return 0L;
    }

    @Override
    public void insert(@NotNull Dataset<Row> data, boolean overwrite)
    {
        this.startTimeNanos = System.nanoTime();
        if (overwrite)
        {
            throw new LoadNotSupportedException("Overwriting existing data needs TRUNCATE on Cassandra, which is not supported");
        }
        maybeEnableTransportExtension();
        writerContext.cluster().checkBulkWriterIsEnabledOrThrow();
        Tokenizer tokenizer = new Tokenizer(writerContext);
        TableSchema tableSchema = writerContext.schema().getTableSchema();
        JavaPairRDD<DecoratedKey, Object[]> sortedRDD = data.toJavaRDD()
                                                            .map(Row::toSeq)
                                                            .map(seq -> JavaConverters.seqAsJavaListConverter(seq).asJava().toArray())
                                                            .map(tableSchema::normalize)
                                                            .keyBy(tokenizer::getDecoratedKey)
                                                            .repartitionAndSortWithinPartitions(broadcastContext.getValue().job().getTokenPartitioner());
        persist(sortedRDD, data.columns());
    }

    public void cancelJob(@NotNull CancelJobEvent cancelJobEvent)
    {
        if (cancelJobEvent.exception != null)
        {
            LOGGER.error("An unrecoverable error occurred during {} stage of import while validating the current cluster state; cancelling job",
                         writeValidator.getPhase(), cancelJobEvent.exception);
        }
        else
        {
            LOGGER.error("Job was canceled due to '{}' during {} stage of import; please rerun import once topology changes are complete",
                         cancelJobEvent.reason, writeValidator.getPhase());
        }
        try
        {
            onCloudStorageTransport(ctx -> abortRestoreJob(ctx, cancelJobEvent.exception));
        }
        finally
        {
            sparkContext.cancelJobGroup(writerContext.job().getId());
        }
    }

    private void persist(@NotNull JavaPairRDD<DecoratedKey, Object[]> sortedRDD, String[] columnNames)
    {
        writeValidator.setPhase("Environment Validation");
        writeValidator.validateInitialEnvironment();
        onDirectTransport(ctx -> writeValidator.setPhase("UploadAndCommit"));
        onCloudStorageTransport(ctx -> writeValidator.setPhase("UploadToCloudStorage"));

        try
        {
            // Copy the broadcast context as a local variable (by passing as the input) to avoid serialization error
            // W/o this, SerializedLambda captures the CassandraBulkSourceRelation object, which is not serializable (required by Spark),
            // as a captured argument. It causes "Task not serializable" error.
            List<StreamResult> results = sortedRDD.mapPartitions(partitionsFlatMapFunc(broadcastContext, columnNames))
                                                  .collect();

            onDirectTransport(ctx -> writeValidator.failIfRingChanged());
            long rowCount = results.stream().mapToLong(res -> res.rowCount).sum();
            LOGGER.info("Bulk writer has written {} rows", rowCount);
            onCloudStorageTransport(context -> {
                // Update with the stream result from tasks.
                // Some token ranges might fail on instances, but the CL is still satisfied at this step
                writeValidator.updateFailureHandler(results);

                List<BlobStreamResult> resultsAsBlobStreamResults = results.stream()
                                                                           .map(BlobStreamResult.class::cast)
                                                                           .collect(Collectors.toList());

                int objectsCount = resultsAsBlobStreamResults.stream()
                                                             .mapToInt(res -> res.createdRestoreSlices.size())
                                                             .sum();
                // report the number of objects persisted on s3
                context.transportExtensionImplementation()
                       .onAllObjectsPersisted(objectsCount, rowCount, getElapsedTimeMillis());
                writeValidator.failIfRingChanged();

                // Unpersist broadcast context to free up executors while driver waits for the
                // import to complete
                unpersist();
                ImportCompletionCoordinator.of(writerContext, writeValidator, resultsAsBlobStreamResults)
                                           .waitForCompletion();
                markRestoreJobAsSucceeded(context);
            });
        }
        catch (Throwable throwable)
        {
            DataTransportInfo transportInfo = writerContext.conf().getTransportInfo();
            LOGGER.error("Bulk Write Failed. {}", transportInfo, throwable);
            onCloudStorageTransport(ctx -> abortRestoreJob(ctx, throwable));
            throw new RuntimeException("Bulk Write to Cassandra has failed. " + transportInfo,
                                       throwable);
        }
        finally
        {
            writeValidator.close();
            onCloudStorageTransport(ignored -> heartbeatReporter.close());
            try
            {
                writerContext.shutdown();
                sqlContext().sparkContext().clearJobGroup();
            }
            catch (Exception ignored)
            {
                // We've made our best effort to close the Bulk Writer context
            }
            // unpersist was already called for the cloud storage transport, only call it if we are in direct
            // transport mode
            onDirectTransport(ctx -> unpersist());
        }
    }

    /**
     * Deletes cached copies of the broadcast on the executors
     */
    private void unpersist()
    {
        try
        {
            LOGGER.info("Unpersisting broadcast context");
            broadcastContext.unpersist(false);
        }
        catch (Throwable throwable)
        {
            if (NonFatal$.MODULE$.apply(throwable))
            {
                LOGGER.error("Uncaught exception in thread {} attempting to unpersist broadcast variable",
                             Thread.currentThread().getName(), throwable);
            }
            else
            {
                throw throwable;
            }
        }
    }

    /**
     * Get a ref copy of BulkWriterContext broadcast variable and compose a function to transform a partition into StreamResult
     *
     * @param ctx BulkWriterContext broadcast variable
     * @return FlatMapFunction
     */
    private static FlatMapFunction<Iterator<Tuple2<DecoratedKey, Object[]>>, StreamResult>
    partitionsFlatMapFunc(Broadcast<BulkWriterContext> ctx, String[] columnNames)
    {
        return iterator -> Collections.singleton(new RecordWriter(ctx.getValue(), columnNames).write(iterator)).iterator();
    }

    // initialization for CloudStorageTransport
    private void maybeEnableTransportExtension()
    {
        onCloudStorageTransport(ctx -> {
            StorageTransportHandler storageTransportHandler = new StorageTransportHandler(ctx, this::cancelJob);
            StorageTransportExtension impl = ctx.transportExtensionImplementation();
            impl.setCredentialChangeListener(storageTransportHandler);
            impl.setObjectFailureListener(storageTransportHandler);
            createRestoreJob(ctx);
            heartbeatReporter.schedule("Extend lease",
                                       TimeUnit.MINUTES.toMillis(1),
                                       () -> extendLeaseForJob(ctx));
        });
    }

    private static void extendLeaseForJob(TransportContext.CloudStorageTransportContext ctx)
    {
        UpdateRestoreJobRequestPayload payload = new UpdateRestoreJobRequestPayload(null, null, null, updatedLeaseTime(ctx));
        try
        {
            ctx.dataTransferApi().updateRestoreJob(payload);
        }
        catch (ClientException e)
        {
            LOGGER.warn("Failed to update expireAt for job", e);
        }
    }

    private static long updatedLeaseTime(TransportContext.CloudStorageTransportContext ctx)
    {
        return System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(ctx.conf().getJobKeepAliveMinutes());
    }

    private long getElapsedTimeMillis()
    {
        long now = System.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(now - this.startTimeNanos);
    }

    void onCloudStorageTransport(Consumer<TransportContext.CloudStorageTransportContext> consumer)
    {
        TransportContext transportContext = writerContext.transportContext();
        if (transportContext instanceof TransportContext.CloudStorageTransportContext)
        {
            consumer.accept((TransportContext.CloudStorageTransportContext) transportContext);
        }
    }

    void onDirectTransport(Consumer<TransportContext.DirectDataBulkWriterContext> consumer)
    {
        TransportContext transportContext = writerContext.transportContext();
        if (transportContext instanceof TransportContext.DirectDataBulkWriterContext)
        {
            consumer.accept((TransportContext.DirectDataBulkWriterContext) transportContext);
        }
    }

    private void createRestoreJob(TransportContext.CloudStorageTransportContext context)
    {
        StorageTransportConfiguration conf = context.transportConfiguration();
        RestoreJobSecrets secrets = conf.getStorageCredentialPair().toRestoreJobSecrets(conf.getReadRegion(),
                                                                                        conf.getWriteRegion());
        JobInfo job = context.job();
        CreateRestoreJobRequestPayload payload = CreateRestoreJobRequestPayload
                                                 .builder(secrets, updatedLeaseTime(context))
                                                 .jobAgent(BuildInfo.APPLICATION_NAME)
                                                 .jobId(job.getRestoreJobId())
                                                 .updateImportOptions(importOptions -> {
                                                     importOptions.verifySSTables(true) // we disallow the end-user to bypass the non-extended verify anymore
                                                                  .extendedVerify(false); // always turn off
                                                 })
                                                 .build();

        try
        {
            context.dataTransferApi().createRestoreJob(payload);
        }
        catch (ClientException e)
        {
            throw new RuntimeException("Failed to create a new restore job on Sidecar", e);
        }
    }

    private void markRestoreJobAsSucceeded(TransportContext.CloudStorageTransportContext context)
    {
        UpdateRestoreJobRequestPayload requestPayload = new UpdateRestoreJobRequestPayload(null, null, RestoreJobStatus.SUCCEEDED, null);
        try
        {
            // Prioritize the call to extension, so onJobSucceeded is always invoked.
            context.transportExtensionImplementation().onJobSucceeded(getElapsedTimeMillis());
            context.dataTransferApi().updateRestoreJob(requestPayload);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to mark the restore job as succeeded. jobId={}", context.job().getRestoreJobId(), e);
            // Do not rethrow - avoid triggering the catch block at the call-site that marks job as failed.
        }
    }

    private void abortRestoreJob(TransportContext.CloudStorageTransportContext context, Throwable cause)
    {
        // Prioritize the call to extension, so onJobFailed is always invoked.
        context.transportExtensionImplementation().onJobFailed(getElapsedTimeMillis(), cause);
        // TODO: it should wait for all individual slices to be cancelled after aborting the job?
        try
        {
            context.dataTransferApi().abortRestoreJob();
        }
        catch (ClientException e)
        {
            throw new RuntimeException("Failed to abort the restore job on Sidecar. jobId: " + context.job().getRestoreJobId(), e);
        }
    }
}
