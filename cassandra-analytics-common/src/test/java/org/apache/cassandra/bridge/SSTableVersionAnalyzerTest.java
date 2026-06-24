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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SSTableVersionAnalyzer}
 */
public class SSTableVersionAnalyzerTest
{
    static Stream<Arguments> highestVersionCases()
    {
        return Stream.of(
            Arguments.of(Collections.singleton("big-oa"), CassandraVersion.FIVEZERO),
            Arguments.of(Collections.singleton("big-na"), CassandraVersion.FOURZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-nb")), CassandraVersion.FOURZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-na", "big-oa")), CassandraVersion.FIVEZERO),
            Arguments.of(new HashSet<>(Arrays.asList("big-oa", "bti-da")), CassandraVersion.FIVEZERO)
        );
    }

    @ParameterizedTest
    @MethodSource("highestVersionCases")
    void testDetermineBridgeVersionForRead(Set<String> versions, CassandraVersion expected)
    {
        assertThat(SSTableVersionAnalyzer.determineBridgeVersionForRead(versions)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("highestVersionCases")
    void testDetermineBridgeVersionForWrite(Set<String> versions, CassandraVersion expected)
    {
        assertThat(SSTableVersionAnalyzer.determineBridgeVersionForWrite(versions, "big")).isEqualTo(expected);
    }

    static Stream<Arguments> nullOrEmptyCases()
    {
        return Stream.of(Arguments.of(Collections.emptySet()), Arguments.of((Set<String>) null));
    }

    @ParameterizedTest
    @MethodSource("nullOrEmptyCases")
    void testNullOrEmptyThrows(Set<String> versions)
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForRead(versions))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("no SSTable versions found on cluster");
    }

    @Test
    void testUnknownVersionThrows()
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForRead(Collections.singleton("unknown-xx")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Unknown SSTable version: unknown-xx");
    }

    @Test
    void testWriteUnsupportedFormatThrows()
    {
        assertThatThrownBy(() -> SSTableVersionAnalyzer.determineBridgeVersionForWrite(Collections.singleton("big-na"), "bti"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster does not support requested SSTable format 'bti'");
    }
}
