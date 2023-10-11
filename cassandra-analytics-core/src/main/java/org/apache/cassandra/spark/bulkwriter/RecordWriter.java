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

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.RowBufferMode;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.util.TaskContextUtils;
import org.apache.spark.TaskContext;
import scala.Tuple2;

@SuppressWarnings({ "ConstantConditions" })
public class RecordWriter implements Serializable
{
    private static final long serialVersionUID = -5937937824967790610L;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);

    private final BulkWriterContext writerContext;
    private final String[] columnNames;
    private final Supplier<TaskContext> taskContextSupplier;
    private final BiFunction<BulkWriterContext, Path, SortedSSTableWriter> tableWriterSupplier;
    // variables updated during `#write(Iterator)`
    private SortedSSTableWriter sstableWriter = null;
    private int batchNumber = 0;

    public RecordWriter(BulkWriterContext writerContext, String[] columnNames)
    {
        this(writerContext, columnNames, TaskContext::get, SortedSSTableWriter::new);
    }

    @VisibleForTesting
    RecordWriter(BulkWriterContext writerContext,
                 String[] columnNames,
                 Supplier<TaskContext> taskContextSupplier,
                 BiFunction<BulkWriterContext, Path, SortedSSTableWriter> tableWriterSupplier)
    {
        this.writerContext = writerContext;
        this.columnNames = columnNames;
        this.taskContextSupplier = taskContextSupplier;
        this.tableWriterSupplier = tableWriterSupplier;

        writerContext.cluster().startupValidate();
    }

    /**
     * Write data into stream
     * @param sourceIterator source data
     * @return stream result
     */
    public StreamResult write(Iterator<Tuple2<DecoratedKey, Object[]>> sourceIterator)
    {
        TaskContext taskContext = taskContextSupplier.get();
        LOGGER.info("[{}]: Processing Bulk Writer partition", taskContext.partitionId());
        Iterator<Tuple2<DecoratedKey, Object[]>> dataIterator = new JavaInterruptibleIterator<>(taskContext, sourceIterator);
        StreamSession<?> streamSession = writerContext.transportContext().createStreamSession(taskContext);
        validateAcceptableTimeSkewOrThrow(streamSession.replicas);
        int partitionId = taskContext.partitionId();
        Range<BigInteger> range = getTokenRange(taskContext);
        JobInfo job = writerContext.job();
        Map<String, Object> valueMap = new HashMap<>();
        Path baseDir = TaskContextUtils.getPartitionUniquePath(taskContext,
                                                               streamSession.sessionID,
                                                               Paths.get(System.getProperty("java.io.tmpdir"),
                                                                         job.getRestoreJobId().toString()));
        try
        {
            while (dataIterator.hasNext())
            {
                maybeCreateTableWriter(partitionId, baseDir);
                writeRow(valueMap, dataIterator.next(), partitionId, range);
                checkBatchSize(streamSession, job, partitionId, !dataIterator.hasNext());
            }

            LOGGER.info("[{}] Done with all writers and waiting for stream to complete", partitionId);
            return streamSession.close();
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}] Failed to write job={}, taskStageAttemptNumber={}, taskAttemptNumber={}",
                         partitionId,
                         job.getId().toString(),
                         taskContext.stageAttemptNumber(),
                         taskContext.attemptNumber());

            if (exception instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(exception);
        }
    }

    private void checkBatchSize(StreamSession<?> streamSession, JobInfo jobInfo, int partitionId, boolean isLast) throws IOException
    {
        // flush when any of the following condition is met
        // 1) having collected enough rows (when using UNBUFFERED mode), or
        // 2) reaching end of data
        if (isLast || hasRowCountReachedBatchSize(jobInfo))
        {
            flush(streamSession, partitionId, isLast);
        }
    }

    /**
     * @param jobInfo job info
     * @return true if having collected enough rows when using UNBUFFERED mode
     */
    private boolean hasRowCountReachedBatchSize(JobInfo jobInfo)
    {
        return jobInfo.getRowBufferMode() == RowBufferMode.UNBUFFERED
               && sstableWriter.rowCount() >= jobInfo.getSstableBatchSize();
    }

    /**
     * Flushes the written rows to the stream and reset the internal SSTableWriter
     * @param streamSession the stream
     * @param isLast indicate whether it is the last flush
     * @throws IOException I/O exceptions during flush
     */
    private void flush(StreamSession<?> streamSession, int partitionId, boolean isLast) throws IOException
    {
        LOGGER.info("[{}][{}] Closing writer and scheduling SStable stream with {} rows",
                    partitionId, batchNumber, sstableWriter.rowCount());
        sstableWriter.close(writerContext, partitionId);
        streamSession.scheduleStream(sstableWriter, isLast);
        sstableWriter = null;
    }

    private Range<BigInteger> getTokenRange(TaskContext taskContext)
    {
        return writerContext.job().getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    private void validateAcceptableTimeSkewOrThrow(List<RingInstance> replicas)
    {
        TimeSkewResponse timeSkewResponse = writerContext.cluster().getTimeSkew(replicas);
        Instant localNow = Instant.now();
        Instant remoteNow = Instant.ofEpochMilli(timeSkewResponse.currentTime);
        Duration range = Duration.ofMinutes(timeSkewResponse.allowableSkewInMinutes);
        if (localNow.isBefore(remoteNow.minus(range)) || localNow.isAfter(remoteNow.plus(range)))
        {
            String message = String.format("Time skew between Spark and Cassandra is too large. "
                                           + "Allowable skew is %d minutes. "
                                           + "Spark executor time is %s, Cassandra instance time is %s",
                                           timeSkewResponse.allowableSkewInMinutes, localNow, remoteNow);
            throw new UnsupportedOperationException(message);
        }
    }

    public void writeRow(Map<String, Object> valueMap,
                         Tuple2<DecoratedKey, Object[]> keyAndData,
                         int partitionId,
                         Range<BigInteger> range) throws IOException
    {
        DecoratedKey key = keyAndData._1();
        BigInteger token = key.getToken();
        Preconditions.checkState(range.contains(token),
                                 String.format("Received Token %s outside of expected range %s", token, range));
        try
        {
            sstableWriter.addRow(token, getBindValuesForColumns(valueMap, columnNames, keyAndData._2()));
        }
        catch (RuntimeException exception)
        {
            String message = String.format("[%s]: Failed to write data to SSTable: SBW DecoratedKey was %s",
                                           partitionId, key);
            LOGGER.error(message, exception);
            throw exception;
        }
    }

    void maybeCreateTableWriter(int partitionId, Path baseDir) throws IOException
    {
        if (sstableWriter != null)
        {
            return;
        }

        Path ssTableDirectory = Paths.get(baseDir.toString()).resolve(Integer.toString(++batchNumber));
        Files.createDirectories(ssTableDirectory);

        sstableWriter = tableWriterSupplier.apply(writerContext, ssTableDirectory);

        LOGGER.info("[{}][{}] Created new SSTable writer with directory={}",
                    partitionId, batchNumber, ssTableDirectory);
    }

    private static Map<String, Object> getBindValuesForColumns(Map<String, Object> map, String[] columnNames, Object[] values)
    {
        assert values.length == columnNames.length : "Number of values does not match the number of columns " + values.length + ", " + columnNames.length;
        for (int i = 0; i < columnNames.length; i++)
        {
            map.put(columnNames[i], values[i]);
        }
        return map;
    }

    // The java version of org.apache.spark.InterruptibleIterator
    // An iterator that wraps around an existing iterator to provide task killing functionality.
    // It works by checking the interrupted flag in TaskContext.
    private static class JavaInterruptibleIterator<T> implements Iterator<T>
    {
        private final TaskContext taskContext;
        private final Iterator<T> delegate;

        JavaInterruptibleIterator(TaskContext taskContext, Iterator<T> delegate)
        {
            this.taskContext = taskContext;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext()
        {
            taskContext.killTaskIfInterrupted();
            return delegate.hasNext();
        }

        @Override
        public T next()
        {
            return delegate.next();
        }
    }
}
