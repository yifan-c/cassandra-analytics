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

package org.apache.cassandra.spark.bulkwriter.util;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.UUID;

import com.google.common.collect.Range;

import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.spark.TaskContext;

public final class TaskContextUtils
{
    private TaskContextUtils()
    {
    }

    public static Range<BigInteger> getTokenRange(TaskContext taskContext, JobInfo job)
    {
        return job.getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    /**
     * Create a new stream ID on each invocation. Stream ID identifies a partition uniquely
     * @param taskContext task context
     * @return a new stream ID
     */
    public static String createStreamId(TaskContext taskContext)
    {
        return String.format("%d-%s", taskContext.partitionId(), UUID.randomUUID());
    }

    /**
     * Create a new retry ID on each invocation. Retry ID identifies a retry attempt uniquely
     * @param taskAttemptId attempt ID read from task context
     * @return a new retry ID
     */
    public static String createRetryId(long taskAttemptId)
    {
        return String.format("%s-%s", taskAttemptId, UUID.randomUUID());
    }

    public static Path getPartitionUniquePath(TaskContext taskContext, String streamId, Path base)
    {
        return base.resolve(streamId)
                   .resolve(Integer.toString(taskContext.stageAttemptNumber()))
                   .resolve(Integer.toString(taskContext.attemptNumber()))
                   .resolve(Integer.toString(taskContext.partitionId()));
    }
}
