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

package org.apache.cassandra.clients;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.HttpClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.client.SidecarInstancesProvider;
import o.a.c.sidecar.client.shaded.client.SimpleSidecarInstancesProvider;
import o.a.c.sidecar.client.shaded.common.utils.HttpRange;

import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.CassandraFileSource;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the {@code TimeoutException: No activity on BufferingInputStream} failure caused by the Sidecar
 * client's HTTP connection pool being silently capped at {@link HttpClientConfig#DEFAULT_MAX_POOL_SIZE}.
 * <p>
 * {@link Sidecar#from} never wires {@link Sidecar.ClientConfig#maxPoolSize()} into the {@link HttpClientConfig}
 * used to build the underlying Vert.x {@code WebClient} (it is only used to size the unrelated Vert.x worker
 * pool), so the real HTTP connection pool stays at the hardcoded default no matter what a user configures.
 * When more concurrent SSTable-component downloads are requested than that hidden cap allows, the excess
 * requests sit queued inside Vert.x waiting for a free connection and never deliver a single byte to their
 * {@link BufferingInputStream} before its inactivity timeout fires.
 */
public class SidecarConnectionPoolExhaustionTest
{
    private static final int ACTUAL_POOL_CAP = HttpClientConfig.DEFAULT_MAX_POOL_SIZE;
    private static final int CONFIGURED_POOL_SIZE = ACTUAL_POOL_CAP * 2;
    private static final int CONCURRENT_DOWNLOADS = ACTUAL_POOL_CAP + 5;
    private static final byte[] SSTABLE_BYTES = {1, 2, 3, 4, 5, 6, 7, 8};
    private static final int CHUNK_SIZE = 2;
    private static final long CHUNK_DELAY_MILLIS = 1000L;
    private static final Duration BUFFERING_TIMEOUT = Duration.ofSeconds(2);
    private static final int VERTX_TIMEOUT_SECONDS = 30;

    @Test
    void concurrentDownloadsExceedingHiddenPoolCapTimeOut() throws IOException
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(CONCURRENT_DOWNLOADS * 2));
        server.createContext("/", SidecarConnectionPoolExhaustionTest::serveSlowly);
        server.start();

        ExecutorService consumers = Executors.newFixedThreadPool(CONCURRENT_DOWNLOADS);
        SidecarClient sidecarClient = null;
        try
        {
            SidecarInstanceImpl instance = new SidecarInstanceImpl("localhost", server.getAddress().getPort());
            SidecarInstancesProvider instancesProvider =
            new SimpleSidecarInstancesProvider(Collections.singletonList(instance));

            // A user configuring double the hidden default, expecting the client to actually honor it
            Sidecar.ClientConfig config = Sidecar.ClientConfig.create(-1,
                                                                      0,
                                                                      0L,
                                                                      0L,
                                                                      CassandraFileSource.DEFAULT_MAX_BUFFER_SIZE,
                                                                      CassandraFileSource.DEFAULT_CHUNK_BUFFER_SIZE,
                                                                      CONFIGURED_POOL_SIZE,
                                                                      VERTX_TIMEOUT_SECONDS,
                                                                      null,
                                                                      Collections.emptyMap(),
                                                                      Collections.emptyMap());
            sidecarClient = Sidecar.from(instancesProvider, config, null);
            SidecarClient client = sidecarClient;

            CassandraFileSource<SSTable> source = buildSource(client, instance);

            List<CompletableFuture<Boolean>> downloads =
            IntStream.range(0, CONCURRENT_DOWNLOADS)
                     .mapToObj(i -> CompletableFuture.supplyAsync(() -> attemptDownload(source), consumers))
                     .collect(Collectors.toList());

            long timedOutCount = downloads.stream()
                                          .map(CompletableFuture::join)
                                          .filter(Boolean::booleanValue)
                                          .count();

            assertThat(timedOutCount)
            .describedAs("Expected some of the %d concurrent SSTable downloads to hit the BufferingInputStream "
                         + "inactivity timeout: maxPoolSize=%d never reaches HttpClientConfig, so the real HTTP "
                         + "connection pool silently stays capped at %d",
                         CONCURRENT_DOWNLOADS, CONFIGURED_POOL_SIZE, ACTUAL_POOL_CAP)
            .isGreaterThan(0);
        }
        finally
        {
            consumers.shutdownNow();
            if (sidecarClient != null)
            {
                try
                {
                    sidecarClient.close();
                }
                catch (Exception ignored)
                {
                    // Best effort cleanup
                }
            }
            server.stop(0);
        }
    }

    /**
     * Drains {@code source} through a fresh {@link BufferingInputStream}.
     *
     * @return true if the download failed with the BufferingInputStream inactivity {@link TimeoutException}
     */
    private static boolean attemptDownload(CassandraFileSource<SSTable> source)
    {
        try (BufferingInputStream<SSTable> inputStream =
             new BufferingInputStream<>(source, BufferingInputStreamStats.doNothingStats()))
        {
            int bytesRead;
            do
            {
                bytesRead = inputStream.read();
            }
            while (bytesRead >= 0);
            return false;
        }
        catch (IOException exception)
        {
            return exception.getCause() instanceof TimeoutException;
        }
    }

    /**
     * Serves the SSTable bytes a couple at a time with a delay in between, holding the HTTP connection open for
     * longer than {@link #BUFFERING_TIMEOUT} but with no single gap between chunks exceeding it, so a request
     * that gets a connection promptly never sees inactivity, while a request still queued for a free connection
     * gets nothing at all.
     */
    private static void serveSlowly(HttpExchange exchange) throws IOException
    {
        try
        {
            exchange.sendResponseHeaders(200, SSTABLE_BYTES.length);
            OutputStream body = exchange.getResponseBody();
            for (int offset = 0; offset < SSTABLE_BYTES.length; offset += CHUNK_SIZE)
            {
                int length = Math.min(CHUNK_SIZE, SSTABLE_BYTES.length - offset);
                body.write(SSTABLE_BYTES, offset, length);
                body.flush();
                if (offset + length < SSTABLE_BYTES.length)
                {
                    Thread.sleep(CHUNK_DELAY_MILLIS);
                }
            }
        }
        catch (InterruptedException exception)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            exchange.close();
        }
    }

    // Bridges the shaded Sidecar client's StreamConsumer/StreamBuffer callbacks onto the
    // org.apache.cassandra.spark.utils.streaming ones expected by BufferingInputStream
    private static CassandraFileSource<SSTable> buildSource(SidecarClient client, SidecarInstanceImpl instance)
    {
        return new CassandraFileSource<SSTable>()
        {
            @Override
            public void request(long start, long end, StreamConsumer consumer)
            {
                client.streamSSTableComponent(instance, "ks", "tbl", "snapshot", "nb-1-big-Data.db",
                                              HttpRange.of(start, end),
                                              new o.a.c.sidecar.client.shaded.client.StreamConsumer()
                                              {
                                                  @Override
                                                  public void onRead(o.a.c.sidecar.client.shaded.client.StreamBuffer buffer)
                                                  {
                                                      int length = buffer.readableBytes();
                                                      byte[] bytes = new byte[length];
                                                      buffer.copyBytes(0, bytes, 0, length);
                                                      consumer.onRead(StreamBuffer.wrap(bytes));
                                                  }

                                                  @Override
                                                  public void onComplete()
                                                  {
                                                      consumer.onEnd();
                                                  }

                                                  @Override
                                                  public void onError(Throwable throwable)
                                                  {
                                                      consumer.onError(throwable);
                                                  }
                                              });
            }

            @Override
            public SSTable cassandraFile()
            {
                return null;
            }

            @Override
            public FileType fileType()
            {
                return null;
            }

            @Override
            public long size()
            {
                return SSTABLE_BYTES.length;
            }

            @Override
            public long maxBufferSize()
            {
                return SSTABLE_BYTES.length;
            }

            @Override
            public long chunkBufferSize()
            {
                return SSTABLE_BYTES.length;
            }

            @Override
            public Duration timeout()
            {
                return BUFFERING_TIMEOUT;
            }
        };
    }
}
