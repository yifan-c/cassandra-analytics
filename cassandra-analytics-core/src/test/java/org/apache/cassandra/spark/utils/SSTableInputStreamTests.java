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

package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;

import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.streaming.SSTableInputStream;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.streaming.SSTableInputStream.timeoutLeftNanos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the {@link SSTableInputStream} by mocking the {@link SSTableSource}
 */
public class SSTableInputStreamTests
{
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);
    private static final ExecutorService EXECUTOR =
            Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("sstable-tests-%d")
                                                                      .setDaemon(true)
                                                                      .build());
    static final int DEFAULT_CHUNK_SIZE = 8192;
    static final Stats STATS = Stats.DoNothingStats.INSTANCE;

    // Mocked Tests

    @Test
    public void testMockedClient() throws IOException
    {
        runMockedTest(1, 1, DEFAULT_CHUNK_SIZE);
        runMockedTest(1, 5, DEFAULT_CHUNK_SIZE * 5);
        runMockedTest(10, 10, SSTableSource.DEFAULT_MAX_BUFFER_SIZE);
        runMockedTest(20, 1024, 33554400L);
        runMockedTest(10, 10, DEFAULT_CHUNK_SIZE * 10);
    }

    private interface SSTableRequest
    {
        void request(long start, long end, StreamConsumer consumer);
    }

    private static SSTableSource<SSTable> buildSource(long size,
                                                      Long maxBufferSize,
                                                      Long requestChunkSize,
                                                      SSTableRequest request,
                                                      Duration duration)
    {
        return new SSTableSource<SSTable>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                request.request(start, end, consumer);
            }

            public SSTable sstable()
            {
                return null;
            }

            public FileType fileType()
            {
                return null;
            }

            public long size()
            {
                return size;
            }

            public long maxBufferSize()
            {
                return maxBufferSize != null ? maxBufferSize : SSTableSource.DEFAULT_MAX_BUFFER_SIZE;
            }

            public long chunkBufferSize()
            {
                return requestChunkSize != null ? requestChunkSize : SSTableSource.DEFAULT_CHUNK_BUFFER_SIZE;
            }

            public Duration timeout()
            {
                return duration;
            }
        };
    }

    // Test SSTableInputStream using mocked SSTableSource
    private void runMockedTest(int numRequests, int chunksPerRequest, long maxBufferSize) throws IOException
    {
        long requestChunkSize = (long) DEFAULT_CHUNK_SIZE * chunksPerRequest;
        long fileSize = requestChunkSize * (long) numRequests;
        AtomicInteger requestCount = new AtomicInteger(0);
        SSTableSource<SSTable> mockedClient = buildSource(fileSize,
                                                          maxBufferSize,
                                                          requestChunkSize,
                                                          (start, end, consumer) -> {
            requestCount.incrementAndGet();
            writeBuffers(consumer, randomBuffers(chunksPerRequest));
        }, null);
        SSTableInputStream<SSTable> is = new SSTableInputStream<>(mockedClient, STATS);
        readStreamFully(is);
        assertEquals(numRequests, requestCount.get());
        assertEquals(0L, is.bytesBuffered());
        assertEquals(fileSize, is.bytesWritten());
        assertEquals(fileSize, is.bytesRead());
    }

    @Test(expected = IOException.class)
    public void testFailure() throws IOException
    {
        int chunksPerRequest = 10;
        int numRequests = 10;
        long length = SSTableSource.DEFAULT_CHUNK_BUFFER_SIZE * chunksPerRequest * numRequests;
        AtomicInteger count = new AtomicInteger(0);
        SSTableSource<SSTable> source = buildSource(length,
                                                    SSTableSource.DEFAULT_MAX_BUFFER_SIZE,
                                                    SSTableSource.DEFAULT_CHUNK_BUFFER_SIZE,
                                                    (start, end, consumer) -> {
            if (count.incrementAndGet() > (numRequests / 2))
            {
                // Halfway through throw random exception
                EXECUTOR.submit(() -> consumer.onError(new RuntimeException("Something bad happened...")));
            }
            else
            {
                writeBuffers(consumer, randomBuffers(chunksPerRequest));
            }
        }, null);
        readStreamFully(new SSTableInputStream<>(source, STATS));
        fail("Should have failed with IOException");
    }

    @Test
    public void testTimeout()
    {
        long now = System.nanoTime();
        assertEquals(Duration.ofMillis(100).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(900).toNanos()));
        assertEquals(Duration.ofMillis(-500).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(1500).toNanos()));
        assertEquals(Duration.ofMillis(995).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(5).toNanos()));
        assertEquals(Duration.ofMillis(1000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(0).toNanos()));
        assertEquals(Duration.ofMillis(1000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now + Duration.ofMillis(500).toNanos()));
        assertEquals(Duration.ofMillis(35000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(60000), now, now - Duration.ofMillis(25000).toNanos()));
        assertEquals(Duration.ofMillis(-5000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(60000), now, now - Duration.ofMillis(65000).toNanos()));
        assertEquals(Duration.ofMillis(0).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(60000), now, now - Duration.ofMillis(60000).toNanos()));
    }

    @Test
    @SuppressWarnings("UnstableApiUsage")
    public void testTimeoutShouldAccountForActivityTime()
    {
        int chunksPerRequest = 10;
        int numRequests = 10;
        long length = SSTableSource.DEFAULT_CHUNK_BUFFER_SIZE * chunksPerRequest * numRequests;
        AtomicInteger count = new AtomicInteger(0);
        Duration timeout = Duration.ofMillis(1000);
        long startTime = System.nanoTime();
        long sleepTimeInMillis = 100L;
        SSTableSource<SSTable> source = buildSource(length,
                                                    SSTableSource.DEFAULT_MAX_BUFFER_SIZE,
                                                    SSTableSource.DEFAULT_CHUNK_BUFFER_SIZE,
                                                    (start, end, consumer) -> {
            // Only respond once so future requests will time out
            if (count.incrementAndGet() == 1)
            {
                EXECUTOR.submit(() -> {
                    Uninterruptibles.sleepUninterruptibly(sleepTimeInMillis, TimeUnit.MILLISECONDS);
                    writeBuffers(consumer, randomBuffers(chunksPerRequest));
                });
            }
        }, timeout);
        try
        {
            readStreamFully(new SSTableInputStream<>(source, STATS));
            fail("Should not reach here, should throw TimeoutException");
        }
        catch (IOException exception)
        {
            assertTrue(exception.getCause() instanceof TimeoutException);
        }
        Duration duration = Duration.ofNanos(System.nanoTime() - startTime);
        Duration maxAcceptable = timeout.plus(Duration.ofMillis(sleepTimeInMillis));
        assertTrue("Timeout didn't account for activity time. "
                 + "Took " + duration.toMillis() + "ms should have taken at most " + maxAcceptable.toMillis() + "ms",
                   duration.minus(maxAcceptable).toMillis() < 100);
    }

    @Test
    public void testSkipOnInit() throws IOException
    {
        int size = 20971520;
        int chunkSize = 1024;
        int numChunks = 16;
        MutableInt bytesRead = new MutableInt(0);
        MutableInt count = new MutableInt(0);
        SSTableSource<SSTable> source = new SSTableSource<SSTable>()
        {
            @Override
            public void request(long start, long end, StreamConsumer consumer)
            {
                assertNotEquals(0, start);
                int length = (int) (end - start + 1);
                consumer.onRead(randomBuffer(length));
                bytesRead.add(length);
                count.increment();
                consumer.onEnd();
            }

            @Override
            public long chunkBufferSize()
            {
                return chunkSize;
            }

            @Override
            public SSTable sstable()
            {
                return null;
            }

            @Override
            public FileType fileType()
            {
                return FileType.INDEX;
            }

            @Override
            public long size()
            {
                return size;
            }

            @Override
            @Nullable
            public Duration timeout()
            {
                return Duration.ofSeconds(5);
            }
        };

        int bytesToRead = chunkSize * numChunks;
        long skipAhead = size - bytesToRead + 1;
        try (SSTableInputStream<SSTable> stream = new SSTableInputStream<>(source, STATS))
        {
            // Skip ahead so we only read the final chunks
            ByteBufferUtils.skipFully(stream, skipAhead);
            readStreamFully(stream);
        }
        // Verify we only read final chunks and not the start of the file
        assertEquals(bytesToRead, bytesRead.intValue());
        assertEquals(numChunks, count.intValue());
    }

    @Test
    public void testSkipToEnd() throws IOException
    {
        SSTableSource<SSTable> source = new SSTableSource<SSTable>()
        {
            @Override
            public void request(long start, long end, StreamConsumer consumer)
            {
                consumer.onRead(randomBuffer((int) (end - start + 1)));
                consumer.onEnd();
            }

            @Override
            public SSTable sstable()
            {
                return null;
            }

            @Override
            public FileType fileType()
            {
                return FileType.INDEX;
            }

            @Override
            public long size()
            {
                return 20971520;
            }

            @Override
            @Nullable
            public Duration timeout()
            {
                return Duration.ofSeconds(5);
            }
        };

        try (SSTableInputStream<SSTable> stream = new SSTableInputStream<>(source, STATS))
        {
            ByteBufferUtils.skipFully(stream, 20971520);
            readStreamFully(stream);
        }
    }

    // Utils

    private static ImmutableList<StreamBuffer> randomBuffers(int count)
    {
        return ImmutableList.copyOf(IntStream.range(0, count)
                                             .mapToObj(buffer -> randomBuffer())
                                             .collect(Collectors.toList()));
    }

    private static StreamBuffer randomBuffer()
    {
        return randomBuffer(DEFAULT_CHUNK_SIZE);
    }

    private static StreamBuffer randomBuffer(int size)
    {
        return StreamBuffer.wrap(RandomUtils.randomBytes(size));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void readStreamFully(SSTableInputStream<SSTable> inputStream) throws IOException
    {
        try (SSTableInputStream<SSTable> in = inputStream)
        {
            while (in.read() >= 0)
            {
                // CHECKSTYLE IGNORE: Do nothing
            }
        }
    }

    private static void writeBuffers(StreamConsumer consumer, ImmutableList<StreamBuffer> buffers)
    {
        if (buffers.isEmpty())
        {
            // No more buffers so finished
            consumer.onEnd();
            return;
        }

        SCHEDULER.schedule(() -> {
            EXECUTOR.submit(() -> {
                // Write next buffer to StreamConsumer
                consumer.onRead(buffers.get(0));
                writeBuffers(consumer, buffers.subList(1, buffers.size()));
            });
        }, RandomUtils.RANDOM.nextInt(50), TimeUnit.MICROSECONDS);  // Inject random latency
    }
}
