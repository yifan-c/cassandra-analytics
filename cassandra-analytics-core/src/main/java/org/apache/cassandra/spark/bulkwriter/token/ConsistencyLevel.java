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

package org.apache.cassandra.spark.bulkwriter.token;

import java.util.Collection;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;

public interface ConsistencyLevel
{
    /**
     * Whether the consistency level only considers replicas in the local data center.
     *
     * @return true if only considering the local replicas; otherwise, return false
     */
    boolean isLocal();

    /**
     * Check consistency level with the collection of the failed instances
     *
     * @param failedInstances the failed instances in the replica set
     * @param replicationFactor replication factor to check with
     * @param localDC the local data center name if required for the check
     * @return true means the consistency level is _definitively_ not satisfied.
     *         Meanwhile, returning false means no conclusion can be drawn
     */
    boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                  ReplicationFactor replicationFactor,
                                  String localDC);

    /**
     * Check consistency level with the collection of the succeeded instances
     *
     * @param succeededInstances the succeeded instances in the replica set
     * @param replicationFactor replication factor to check with
     * @param localDC the local data center name if required for the check
     * @return true means the consistency level is _definitively_ satisfied.
     *         Meanwhile, returning false means no conclusion can be drawn
     */
    boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                     ReplicationFactor replicationFactor,
                                     String localDC);

    default void ensureNetworkTopologyStrategy(ReplicationFactor replicationFactor, CL cl)
    {
        Preconditions.checkArgument(replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                    cl.name() + " only make sense for NetworkTopologyStrategy keyspaces");
    }

    enum CL implements ConsistencyLevel
    {
        ALL
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                return !failedInstances.isEmpty();
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                // The effective RF during expansion could be larger than the defined RF
                // The check for CL satisfaction should consider the scenario and use >=
                return succeededInstances.size() >= rf;
            }
        },
        EACH_QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, EACH_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                for (String datacenter : replicationFactor.getOptions().keySet())
                {
                    int rf = replicationFactor.getOptions().get(datacenter);
                    if (failedInstances.stream()
                                       .filter(instance -> instance.getDataCenter().equalsIgnoreCase(datacenter))
                                       .count() > rf - (rf / 2 + 1))
                    {
                        return true;
                    }
                }

                return false;
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, EACH_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                for (String datacenter : replicationFactor.getOptions().keySet())
                {
                    int rf = replicationFactor.getOptions().get(datacenter);
                    int majority = rf / 2 + 1;
                    if (succeededInstances.stream()
                                          .filter(instance -> instance.getDataCenter().equalsIgnoreCase(datacenter))
                                          .count() < majority)
                    {
                        return false;
                    }
                }
                return true;
            }
        },
        QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return failedInstances.size() > rf - (rf / 2 + 1);
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return succeededInstances.size() > rf / 2;
            }
        },
        LOCAL_QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return true;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                int rf = replicationFactor.getOptions().get(localDC);
                return failedInstances.stream()
                                      .filter(instance -> instance.getDataCenter().equalsIgnoreCase(localDC))
                                      .count() > rf - (rf / 2 + 1);
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_QUORUM);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                int rf = replicationFactor.getOptions().get(localDC);
                return succeededInstances.stream()
                                         .filter(instance -> instance.getDataCenter().equalsIgnoreCase(localDC))
                                         .count() > rf / 2;
            }
        },
        ONE
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return failedInstances.size() > rf - 1;
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                return !succeededInstances.isEmpty();
            }
        },
        TWO
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                int rf = replicationFactor.getTotalReplicationFactor();
                return failedInstances.size() > rf - 2;
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                return succeededInstances.size() >= 2;
            }
        },
        LOCAL_ONE
        {
            @Override
            public boolean isLocal()
            {
                return true;
            }

            @Override
            public boolean hasDefinitivelyFailed(Collection<? extends CassandraInstance> failedInstances,
                                                 ReplicationFactor replicationFactor,
                                                 String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_ONE);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                int rf = replicationFactor.getOptions().get(localDC);
                return failedInstances.stream()
                                      .filter(instance -> instance.getDataCenter().equalsIgnoreCase(localDC))
                                      .count() > (rf - 1);
            }

            public boolean hasDefinitivelySatisfied(Collection<? extends CassandraInstance> succeededInstances,
                                                    ReplicationFactor replicationFactor,
                                                    String localDC)
            {
                ensureNetworkTopologyStrategy(replicationFactor, LOCAL_ONE);
                Objects.requireNonNull(localDC, "localDC cannot be null");

                return succeededInstances.stream().anyMatch(instance -> instance.getDataCenter().equalsIgnoreCase(localDC));
            }
        }
    }
}
