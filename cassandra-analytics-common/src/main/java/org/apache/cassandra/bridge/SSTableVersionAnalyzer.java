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

package org.apache.cassandra.bridge;

import java.util.Comparator;
import java.util.Set;

/**
 * Determines the Cassandra bridge version to load from the SSTable versions found on a cluster.
 *
 * <p>The bridge is selected from the highest SSTable version detected on the cluster. Callers are
 * responsible for deciding whether SSTable-version-based selection is enabled and for supplying a
 * fallback otherwise.</p>
 */
public final class SSTableVersionAnalyzer
{
    private SSTableVersionAnalyzer()
    {
    }

    /**
     * Determines the bridge version for a write operation and verifies it can produce the requested format.
     *
     * @param sstableVersionsOnCluster SSTable versions found on cluster nodes
     * @param requestedFormat          requested SSTable format, e.g. "big" or "bti"
     * @return the highest {@link CassandraVersion} found on the cluster
     * @throws IllegalStateException         if the versions are empty or unrecognized
     * @throws UnsupportedOperationException if the determined version cannot write the requested format
     */
    public static CassandraVersion determineBridgeVersionForWrite(Set<String> sstableVersionsOnCluster, String requestedFormat)
    {
        CassandraVersion bridgeVersion = determineBridgeVersionForRead(sstableVersionsOnCluster);
        if (!bridgeVersion.sstableFormats().contains(requestedFormat))
        {
            throw new UnsupportedOperationException(String.format(
                "Cluster does not support requested SSTable format '%s'. Bridge version determined is %s, "
                + "which only supports formats: %s",
                requestedFormat, bridgeVersion.versionName(), bridgeVersion.sstableFormats()));
        }
        return bridgeVersion;
    }

    /**
     * Determines the bridge version for a read operation from the highest SSTable version on the cluster.
     *
     * @param sstableVersionsOnCluster SSTable versions found on cluster nodes
     * @return the highest {@link CassandraVersion} found on the cluster
     * @throws IllegalStateException if the versions are empty or unrecognized
     */
    public static CassandraVersion determineBridgeVersionForRead(Set<String> sstableVersionsOnCluster)
    {
        if (sstableVersionsOnCluster == null || sstableVersionsOnCluster.isEmpty())
        {
            throw new IllegalStateException("Unable to determine bridge version: no SSTable versions found on cluster");
        }

        return sstableVersionsOnCluster.stream()
                                       .map(SSTableVersionAnalyzer::toCassandraVersion)
                                       .max(Comparator.comparingInt(CassandraVersion::versionNumber))
                                       .orElseThrow(() -> new IllegalStateException("Unable to find highest SSTable version"));
    }

    private static CassandraVersion toCassandraVersion(String sstableVersion)
    {
        return CassandraVersion.fromSSTableVersion(sstableVersion)
                               .orElseThrow(() -> new IllegalStateException("Unknown SSTable version: " + sstableVersion));
    }
}
