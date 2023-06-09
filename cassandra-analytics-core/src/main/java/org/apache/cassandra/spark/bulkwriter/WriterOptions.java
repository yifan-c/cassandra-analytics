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

public enum WriterOptions implements WriterOption
{
    SIDECAR_INSTANCES,
    KEYSPACE,
    TABLE,
    BULK_WRITER_CL,
    LOCAL_DC,
    NUMBER_SPLITS,
    BATCH_SIZE,
    COMMIT_THREADS_PER_INSTANCE,
    COMMIT_BATCH_SIZE,
    VALIDATE_SSTABLES,
    SKIP_EXTENDED_VERIFY,
    WRITE_MODE,
    KEYSTORE_PASSWORD,
    KEYSTORE_PATH,
    KEYSTORE_BASE64_ENCODED,
    KEYSTORE_TYPE,
    TRUSTSTORE_PASSWORD,
    TRUSTSTORE_TYPE,
    TRUSTSTORE_PATH,
    TRUSTSTORE_BASE64_ENCODED,
    SIDECAR_PORT,
    ROW_BUFFER_MODE,
    SSTABLE_DATA_SIZE_IN_MB,
    TTL,
    TIMESTAMP,
    DATA_TRANSPORT,
    DATA_TRANSPORT_EXTENSION_CLASS,
    STORAGE_CLIENT_CONCURRENCY,
    STORAGE_CLIENT_THREAD_KEEP_ALIVE_SECONDS,
    STORAGE_CLIENT_MAX_CHUNK_SIZE_IN_BYTES,
    STORAGE_CLIENT_HTTPS_PROXY,
    STORAGE_CLIENT_ENDPOINT_OVERRIDE,
    MAX_SIZE_PER_SSTABLE_BUNDLE_IN_BYTES_S3_TRANSPORT,
    JOB_KEEP_ALIVE_MINUTES,
    JOB_ID,
    STORAGE_CLIENT_NIO_HTTP_CLIENT_CONNECTION_ACQUISITION_TIMEOUT_SECONDS,
    STORAGE_CLIENT_NIO_HTTP_CLIENT_MAX_CONCURRENCY,
}
