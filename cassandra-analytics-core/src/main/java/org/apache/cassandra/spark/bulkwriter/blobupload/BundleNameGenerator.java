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

package org.apache.cassandra.spark.bulkwriter.blobupload;

import java.math.BigInteger;
import java.util.UUID;

/**
 * Generator to generate names for SSTable bundles. Partition id is only used to get group. In name we include
 * session id instead because session id represents bundles related to a task more uniquely. When task retries happen
 * there is a possibility of same partition id being used with same task attempt number.
 */
public class BundleNameGenerator
{
    private final String nameFormat;

    public BundleNameGenerator(UUID jobId, int partitionId, String sessionId, String retry)
    {
        char prefix = generatePrefixChar(partitionId);
        this.nameFormat = prefix + "_" + jobId + "_" + sessionId + "_" + retry + "_%s_%s";
    }

    /**
     * We want to introduce variability in starting character of zip file name, to guarantee entropy on the object to
     * reduce the number of 503s returned by their service
     * <p>
     * We use 62 for mod, because 62 = 26 (lower case alphabets) + 26 (upper case alphabets) + 10 (digits)
     * For e.g. partitionId = 512 will map to lower case alphabet q
     * </p>
     * @param partitionId spark task's partition id
     * @return starting character to be used while naming zipped SSTables file
     */
    private char generatePrefixChar(int partitionId)
    {
        int group = partitionId % 62;
        if (group <= 25)
        {
            return (char) ('a' + group);
        }
        else if (group <= 51)
        {
            return (char) ('A' + group - 26);
        }
        else
        {
            return (char) ('0' + group - 52);
        }
    }

    public String generate(BigInteger startToken, BigInteger endToken)
    {
        return String.format(nameFormat, startToken, endToken);
    }
}
