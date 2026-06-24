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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Determines the Cassandra bridge version to load from the SSTable versions found on a cluster.
 *
 * <p>For both reads and writes the cluster's SSTable versions must be <em>mutually compatible</em>: every
 * version present must be readable by the highest version present (Cassandra reads its own and older SSTable
 * versions within a bounded compatibility window, see {@link CassandraVersion#getSupportedSStableVersionsForRead()}).
 * If they are not, no single bridge can serve the whole cluster and selection fails rather than silently picking
 * a version. For example a cluster reporting both a very old and a very new version - where the new nodes can no
 * longer read the old SSTables - is rejected.</p>
 *
 * <p>Callers are responsible for deciding whether SSTable-version-based selection is enabled and for
 * supplying a fallback otherwise.</p>
 */
public final class SSTableVersionAnalyzer
{
    private SSTableVersionAnalyzer()
    {
    }

    /**
     * Determines the bridge version for a write operation and verifies it can produce the requested format.
     *
     * <p>The <em>lowest</em> compatible version on the cluster is chosen: a node can import its own and older
     * SSTable versions, but not newer ones, so writing at the lowest version keeps the SSTables importable by
     * every node in the cluster.</p>
     *
     * @param sstableVersionsOnCluster SSTable versions found on cluster nodes
     * @param requestedFormat          requested SSTable format, e.g. "big" or "bti"
     * @return the lowest {@link CassandraVersion} found on the cluster
     * @throws IllegalStateException         if the versions are empty or unrecognized
     * @throws UnsupportedOperationException if the versions are not mutually compatible, or the determined
     *                                       version cannot write the requested format
     */
    public static CassandraVersion determineBridgeVersionForWrite(Set<String> sstableVersionsOnCluster, String requestedFormat)
    {
        ensureMutuallyCompatible(sstableVersionsOnCluster);
        CassandraVersion bridgeVersion = cassandraVersions(sstableVersionsOnCluster)
                                         .min(Comparator.comparingInt(CassandraVersion::versionNumber))
                                         .orElseThrow(() -> new IllegalStateException("Unable to find lowest SSTable version"));
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
     * Determines the bridge version for a read operation: the <em>highest</em> compatible SSTable version on the
     * cluster, so the bridge can read every (older) SSTable version present.
     *
     * @param sstableVersionsOnCluster SSTable versions found on cluster nodes
     * @return the highest {@link CassandraVersion} found on the cluster
     * @throws IllegalStateException         if the versions are empty or unrecognized
     * @throws UnsupportedOperationException if the versions are not mutually compatible
     */
    public static CassandraVersion determineBridgeVersionForRead(Set<String> sstableVersionsOnCluster)
    {
        return ensureMutuallyCompatible(sstableVersionsOnCluster);
    }

    /**
     * Verifies that every SSTable version on the cluster is readable by the highest version present, i.e. they
     * all fall within a single compatibility window so one bridge can serve the whole cluster.
     *
     * @return the highest {@link CassandraVersion} present (the only version able to read all the others)
     */
    private static CassandraVersion ensureMutuallyCompatible(Set<String> sstableVersionsOnCluster)
    {
        CassandraVersion highest = cassandraVersions(sstableVersionsOnCluster)
                                   .max(Comparator.comparingInt(CassandraVersion::versionNumber))
                                   .orElseThrow(() -> new IllegalStateException("Unable to find highest SSTable version"));

        Set<String> readable = highest.getSupportedSStableVersionsForRead();
        List<String> incompatible = sstableVersionsOnCluster.stream()
                                                            .filter(version -> !readable.contains(version))
                                                            .collect(Collectors.toList());
        if (!incompatible.isEmpty())
        {
            throw new UnsupportedOperationException(String.format(
                "SSTable versions on the cluster are not mutually compatible: %s cannot be read by the highest "
                + "version present (%s, which reads %s). Observed SSTable versions: %s",
                incompatible, highest.versionName(), readable, sstableVersionsOnCluster));
        }
        return highest;
    }

    private static Stream<CassandraVersion> cassandraVersions(Set<String> sstableVersionsOnCluster)
    {
        if (sstableVersionsOnCluster == null || sstableVersionsOnCluster.isEmpty())
        {
            throw new IllegalStateException("Unable to determine bridge version: no SSTable versions found on cluster");
        }

        return sstableVersionsOnCluster.stream().map(SSTableVersionAnalyzer::toCassandraVersion);
    }

    private static CassandraVersion toCassandraVersion(String sstableVersion)
    {
        return CassandraVersion.fromSSTableVersion(sstableVersion)
                               .orElseThrow(() -> new IllegalStateException("Unknown SSTable version: " + sstableVersion));
    }
}
