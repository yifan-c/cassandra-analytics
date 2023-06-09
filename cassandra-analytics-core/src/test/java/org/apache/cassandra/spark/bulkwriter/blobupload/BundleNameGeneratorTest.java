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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BundleNameGeneratorTest
{
    @Test
    public void testNameGenerated()
    {
        UUID jobId = UUID.fromString("ea3b3e6b-0d78-4913-89f2-15fcf98711d0");

        int partitionId = 1;
        String sessionId = "1-9062a40b-41ae-40b0-8ba6-47f9bbec6cba";
        String retry = "1-7cd82ff9-d276-11ed-93e5-7fce0df1306f";
        BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, partitionId, sessionId, retry);

        String expectedName = "b_ea3b3e6b-0d78-4913-89f2-15fcf98711d0_1-9062a40b-41ae-40b0-8ba6-47f9bbec6cba_1-7cd82ff9-d276-11ed-93e5-7fce0df1306f_1_3";
        assertEquals(expectedName, nameGenerator.generate(BigInteger.valueOf(1L), BigInteger.valueOf(3L)));

        partitionId = 512;
        nameGenerator = new BundleNameGenerator(jobId, partitionId, sessionId, retry);

        expectedName = "q_ea3b3e6b-0d78-4913-89f2-15fcf98711d0_1-9062a40b-41ae-40b0-8ba6-47f9bbec6cba_1-7cd82ff9-d276-11ed-93e5-7fce0df1306f_1_3";
        assertEquals(expectedName, nameGenerator.generate(BigInteger.valueOf(1L), BigInteger.valueOf(3L)));
    }

    @Test
    public void testAllStartCharsGenerated()
    {
        UUID jobId = UUID.fromString("ea3b3e6b-0d78-4913-89f2-15fcf98711d0");
        char[] expectedResults = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

        String retry = "1-7cd82ff9-d276-11ed-93e5-7fce0df1306f";

        // till 61 because of mod 62 results possible
        for (int i = 0; i < 62; i++)
        {
            String sessionId = i + "-9062a40b-41ae-40b0-8ba6-47f9bbec6cba";
            BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, i, sessionId, retry);
            assertEquals(expectedResults[i], nameGenerator.generate(BigInteger.ONE, BigInteger.TEN).charAt(0));
        }
    }
}
