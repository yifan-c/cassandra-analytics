# Rio Pipelines

This document explains the RIO pipelines in the project, how pipelines are triggered, and how they interact with each
other.

# Develop Pipelines

Once a new commit is merged to the [develop](https://github.pie.apple.com/aci/cassandra-analytics-core/tree/apple) branch,
the following pipelines will be triggered.

```plaintext
           ┌────────────────────────────────────────────┐
┌─┐  ┌────►│ cassandra-analytics-core-2.11-jdk8         │
│M│  │     └────────────────────────────────────────────┘
│e│  │     ┌────────────────────────────────────────────┐
│r│  ├────►│ cassandra-analytics-core-2.12-jdk8         │
│g│  │     └────────────────────────────────────────────┘
│e│  │     ┌────────────────────────────────────────────┐
│ ├──┼────►│ cassandra-analytics-core-2.11-jdk11        │
│d│  │     └────────────────────────────────────────────┘
│e│  │     ┌────────────────────────────────────────────┐
│v│  ├────►│ cassandra-analytics-core-2.12-jdk11        │
│e│  │     └────────────────────────────────────────────┘
│l│  │     ┌────────────────────────────────────────────┐
│o│  ├────►│ cassandra-analytics-core-2.12-spark3-jdk11 │
│P│  │     └────────────────────────────────────────────┘
└─┘  │     ┌────────────────────────────────────────────┐
     └────►│ cassandra-analytics-core-2.13-spark3-jdk11 │
           └────────────────────────────────────────────┘
```

# Release Pipelines

The release pipelines need to be manually triggered to produce new release artifacts.

```plaintext
                          ┌───────────────────────────┐
                    ┌────►│ release-2.11-jdk8         │
                    │     └───────────────────────────┘
                    │     ┌───────────────────────────┐
                    ├────►│ release-2.12-jdk8         │
                    │     └───────────────────────────┘
┌────────────────┐  │     ┌───────────────────────────┐
│ Manual trigger │──┼────►│ release-2.11-jdk11        │
└────────────────┘  │     └───────────────────────────┘
                    │     ┌───────────────────────────┐
                    ├────►│ release-2.12-jdk11        │
                    │     └───────────────────────────┘
                    │     ┌───────────────────────────┐
                    ├────►│ release-2.12-spark3-jdk11 │
                    │     └───────────────────────────┘
                    │     ┌───────────────────────────┐
                    └────►│ release-2.13-spark3-jdk11 │
                          └───────────────────────────┘
```
