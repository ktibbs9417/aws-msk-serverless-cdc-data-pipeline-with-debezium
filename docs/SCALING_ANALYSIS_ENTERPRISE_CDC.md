# Enterprise CDC Pipeline Scaling Analysis

## Executive Summary

This document analyzes the feasibility and architectural changes required to scale the current POC CDC pipeline to support:

- **11 Aurora MySQL Clusters**
- **7,600 Databases**
- **2.86 Million Tables**
- **350 Schemas**

**Verdict**: The current single-connector architecture will NOT scale to this workload. A multi-region, multi-account, sharded architecture is required.

---

## Current POC Architecture Limitations

| Component | Current POC | Target Scale | Gap |
|-----------|-------------|--------------|-----|
| Aurora MySQL Clusters | 1 | 11 | 11x |
| Databases | 1 (`testdb`) | 7,600 | 7,600x |
| Tables | 1 (`retail_trans`) | 2,860,000 | 2.86M x |
| Schemas | 1 | 350 | 350x |
| MSK Connectors | 1 | Multiple | Significant |
| Kafka Topics | ~3 | ~2.86M+ | Massive |

---

## AWS Service Quota Analysis

### 1. MSK Serverless Quotas (Per Cluster)

| Quota | Limit | Your Requirement | Status |
|-------|-------|------------------|--------|
| Max partitions (non-compacted) | **2,400** | ~2.86M topics × 3 partitions = **8.58M** | ❌ **BLOCKER** |
| Max partitions (compacted) | **120** | N/A | - |
| Max serverless clusters per account | **10** (adjustable) | Multiple | ⚠️ Needs increase |
| Max ingress throughput | 200 MBps | TBD based on CDC volume | ⚠️ Monitor |
| Max egress throughput | 400 MBps | TBD | ⚠️ Monitor |
| Max consumer groups | 500 | Multiple Firehose streams | ⚠️ Monitor |
| Max client connections | 3,000 | Multiple connectors | ⚠️ Monitor |

**Critical Issue**: MSK Serverless has a hard limit of **2,400 partitions per cluster**. With 2.86M tables, each generating at least one topic, this is a fundamental blocker.

### 2. MSK Connect Quotas (Per Account)

| Quota | Limit | Your Requirement | Status |
|-------|-------|------------------|--------|
| Max connectors (workers) | **60** | 11+ connectors minimum | ⚠️ May need increase |
| Max workers per connector | **10** | Debezium MySQL = 1 task only | ⚠️ Limitation |
| Max custom plugins | **100** | ~1-5 | ✅ OK |
| Max worker configurations | **100** | ~11-50 | ✅ OK |

**Critical Issue**: Debezium MySQL connector only supports `tasks.max=1`. Each connector can only have ONE task, meaning you need **one connector per Aurora cluster** at minimum.

### 3. Glue Schema Registry Quotas (Per Region)

| Quota | Limit | Your Requirement | Status |
|-------|-------|------------------|--------|
| Max Schema Registries | **100** (not adjustable) | 350 schemas → multiple registries | ❌ **BLOCKER** |
| Max Schema Versions | **10,000** (not adjustable) | 2.86M tables = 2.86M+ schemas | ❌ **BLOCKER** |
| Schema version metadata | 10 key-value pairs per version | OK | ✅ OK |

**Critical Issue**: Glue Schema Registry has hard limits of **100 registries** and **10,000 schema versions per region**. With 2.86M tables, each requiring a schema, this is a fundamental blocker.

### 4. Amazon Data Firehose Quotas (Per Region)

| Quota | Limit (us-west-2) | Your Requirement | Status |
|-------|-------------------|------------------|--------|
| Max Firehose streams | **5,000** | 2.86M topics | ❌ **BLOCKER** |
| Max active partitions (dynamic) | **500** | Multiple | ⚠️ Design consideration |
| Throughput per partition | 1 GB/sec | OK | ✅ OK |

**Critical Issue**: You cannot create 2.86M Firehose streams. Maximum is 5,000 per region.

---

## Recommended Architecture for Enterprise Scale

### Option 1: Multi-Account, Multi-Region Sharded Architecture (Recommended)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CONTROL PLANE (Central Account)                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │ Schema Registry │  │ Config Store    │  │ Monitoring      │              │
│  │ (Confluent/     │  │ (DynamoDB)      │  │ (CloudWatch)    │              │
│  │  Apicurio)      │  │                 │  │                 │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
│   DATA PLANE 1    │    │   DATA PLANE 2    │    │   DATA PLANE N    │
│   (Account 1)     │    │   (Account 2)     │    │   (Account N)     │
│                   │    │                   │    │                   │
│ Aurora 1-3        │    │ Aurora 4-6        │    │ Aurora 7-11       │
│ MSK Provisioned   │    │ MSK Provisioned   │    │ MSK Provisioned   │
│ MSK Connect ×3    │    │ MSK Connect ×3    │    │ MSK Connect ×4    │
│ Firehose Streams  │    │ Firehose Streams  │    │ Firehose Streams  │
└─────────┬─────────┘    └─────────┬─────────┘    └─────────┬─────────┘
          │                        │                        │
          └────────────────────────┼────────────────────────┘
                                   ▼
                    ┌─────────────────────────────┐
                    │     CENTRAL DATA LAKE       │
                    │     (S3 Cross-Account)      │
                    │                             │
                    │  s3://cdc-data-lake/        │
                    │    ├── cluster-1/           │
                    │    ├── cluster-2/           │
                    │    └── ...                  │
                    └─────────────────────────────┘
```

### Option 2: MSK Provisioned with Topic Aggregation

Instead of one topic per table, aggregate CDC events:

```
┌─────────────────────────────────────────────────────────────────┐
│                    TOPIC AGGREGATION STRATEGY                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Instead of: retail-server.db1.table1                           │
│              retail-server.db1.table2                           │
│              ... (2.86M topics)                                 │
│                                                                  │
│  Use:        cdc-events.cluster-1  (all tables from cluster 1)  │
│              cdc-events.cluster-2  (all tables from cluster 2)  │
│              ... (11 topics)                                    │
│                                                                  │
│  With routing key: {database}.{table}                           │
│  Partitions: 100-500 per topic based on throughput              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Detailed Component Changes

### 1. Replace MSK Serverless with MSK Provisioned

**Why**: MSK Serverless partition limits (2,400) are too restrictive.

**MSK Provisioned Capacity Planning**:

| Broker Size | Max Partitions/Broker | Brokers Needed | Total Partitions |
|-------------|----------------------|----------------|------------------|
| kafka.m5.4xlarge | 4,000 | 30 | 120,000 |
| kafka.m5.8xlarge | 8,000 | 30 | 240,000 |
| kafka.m5.12xlarge | 12,000 | 30 | 360,000 |

**Recommendation**: Use `kafka.m5.8xlarge` with 30 brokers across 3 AZs for ~240,000 partitions per cluster.

**CDK Changes** (`cdk_stacks/msk_provisioned.py` - NEW FILE):

```python
#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import aws_cdk as cdk
from aws_cdk import (
  Stack,
  aws_ec2,
  aws_msk,
  aws_logs,
)
from constructs import Construct


class MSKProvisionedStack(Stack):

  def __init__(self, scope: Construct, construct_id: str, vpc, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    msk_cluster_name = self.node.try_get_context("msk_cluster_name")
    
    # Security Groups
    sg_msk_cluster = aws_ec2.SecurityGroup(self, 'MSKSecurityGroup',
      vpc=vpc,
      allow_all_outbound=True,
      description='Security group for MSK Provisioned Cluster'
    )
    
    # Allow internal cluster communication
    sg_msk_cluster.add_ingress_rule(
      peer=sg_msk_cluster,
      connection=aws_ec2.Port.tcp_range(9092, 9098),
      description='Internal cluster communication'
    )

    # CloudWatch Logs for broker logs
    broker_logs = aws_logs.LogGroup(self, 'MSKBrokerLogs',
      log_group_name=f'/aws/msk/{msk_cluster_name}',
      retention=aws_logs.RetentionDays.ONE_WEEK,
      removal_policy=cdk.RemovalPolicy.DESTROY
    )

    # MSK Provisioned Cluster
    msk_cluster = aws_msk.CfnCluster(self, 'MSKProvisionedCluster',
      cluster_name=msk_cluster_name,
      kafka_version='3.5.1',
      number_of_broker_nodes=9,  # 3 per AZ × 3 AZs
      broker_node_group_info=aws_msk.CfnCluster.BrokerNodeGroupInfoProperty(
        instance_type='kafka.m5.4xlarge',
        client_subnets=vpc.select_subnets(
          subnet_type=aws_ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnet_ids[:3],  # One subnet per AZ
        security_groups=[sg_msk_cluster.security_group_id],
        storage_info=aws_msk.CfnCluster.StorageInfoProperty(
          ebs_storage_info=aws_msk.CfnCluster.EBSStorageInfoProperty(
            volume_size=1000  # 1TB per broker
          )
        )
      ),
      client_authentication=aws_msk.CfnCluster.ClientAuthenticationProperty(
        sasl=aws_msk.CfnCluster.SaslProperty(
          iam=aws_msk.CfnCluster.IamProperty(enabled=True)
        )
      ),
      encryption_info=aws_msk.CfnCluster.EncryptionInfoProperty(
        encryption_in_transit=aws_msk.CfnCluster.EncryptionInTransitProperty(
          client_broker='TLS',
          in_cluster=True
        )
      ),
      enhanced_monitoring='PER_TOPIC_PER_PARTITION',
      logging_info=aws_msk.CfnCluster.LoggingInfoProperty(
        broker_logs=aws_msk.CfnCluster.BrokerLogsProperty(
          cloud_watch_logs=aws_msk.CfnCluster.CloudWatchLogsProperty(
            enabled=True,
            log_group=broker_logs.log_group_name
          )
        )
      )
    )

    self.cluster_arn = msk_cluster.attr_arn
    self.cluster_name = msk_cluster.cluster_name
    self.sg_msk_cluster = sg_msk_cluster
```

### 2. Replace Glue Schema Registry with Confluent Schema Registry or Apicurio

**Why**: Glue Schema Registry limits (100 registries, 10,000 versions) cannot support 2.86M schemas.

**Options**:

| Solution | Pros | Cons |
|----------|------|------|
| **Confluent Schema Registry** | Battle-tested, unlimited schemas, Debezium native support | Cost (Confluent Cloud) or operational overhead (self-managed) |
| **Apicurio Registry** | Open source, unlimited schemas, Avro/JSON/Protobuf | Self-managed on EKS/ECS |
| **AWS Glue (Multi-Region)** | Native AWS | Complex, still limited per region |

**Recommendation**: Deploy **Confluent Schema Registry** on Amazon EKS or use **Confluent Cloud**.

**Debezium Configuration for Confluent Schema Registry**:

```properties
# Worker configuration for Confluent Schema Registry
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://schema-registry.internal:8081
key.converter.basic.auth.credentials.source=USER_INFO
key.converter.basic.auth.user.info=${secretManager:schema-registry-creds:username}:${secretManager:schema-registry-creds:password}

value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry.internal:8081
value.converter.basic.auth.credentials.source=USER_INFO
value.converter.basic.auth.user.info=${secretManager:schema-registry-creds:username}:${secretManager:schema-registry-creds:password}
```

### 3. MSK Connect Sharding Strategy

**Why**: One Debezium MySQL connector = one task = one Aurora cluster.

**Architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                   MSK CONNECT DEPLOYMENT                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Connector 1  ──▶  Aurora Cluster 1  (691 DBs, 260K tables)     │
│  Connector 2  ──▶  Aurora Cluster 2  (691 DBs, 260K tables)     │
│  Connector 3  ──▶  Aurora Cluster 3  (691 DBs, 260K tables)     │
│  ...                                                             │
│  Connector 11 ──▶  Aurora Cluster 11 (691 DBs, 260K tables)     │
│                                                                  │
│  Total: 11 Connectors (within 60 worker quota)                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**CDK Changes** - Create connector factory (`cdk_stacks/kafka_connector_factory.py`):

```python
#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

from typing import List, Dict
import aws_cdk as cdk
from aws_cdk import (
  Stack,
  aws_kafkaconnect,
  aws_iam,
  aws_logs,
)
from constructs import Construct


class KafkaConnectorFactoryStack(Stack):
  """Creates multiple Debezium connectors for multiple Aurora clusters."""

  def __init__(self, scope: Construct, construct_id: str,
    vpc,
    aurora_clusters: List[Dict],  # [{name, hostname, credentials_arn, databases}]
    msk_bootstrap_servers: str,
    custom_plugin_arn: str,
    worker_config_arn: str,
    schema_registry_url: str,
    **kwargs) -> None:

    super().__init__(scope, construct_id, **kwargs)

    for idx, cluster in enumerate(aurora_clusters):
      self._create_connector(
        index=idx,
        cluster_name=cluster['name'],
        db_hostname=cluster['hostname'],
        credentials_arn=cluster['credentials_arn'],
        database_include_list=cluster['databases'],
        msk_bootstrap_servers=msk_bootstrap_servers,
        custom_plugin_arn=custom_plugin_arn,
        worker_config_arn=worker_config_arn,
        schema_registry_url=schema_registry_url,
        vpc=vpc
      )

  def _create_connector(self, index, cluster_name, db_hostname, 
    credentials_arn, database_include_list, msk_bootstrap_servers,
    custom_plugin_arn, worker_config_arn, schema_registry_url, vpc):
    
    connector_name = f'debezium-{cluster_name}'
    
    log_group = aws_logs.LogGroup(self, f'ConnectorLogs{index}',
      log_group_name=f'/aws/msk-connect/{connector_name}',
      retention=aws_logs.RetentionDays.THREE_DAYS,
      removal_policy=cdk.RemovalPolicy.DESTROY
    )

    # IAM Role for connector
    connector_role = aws_iam.Role(self, f'ConnectorRole{index}',
      assumed_by=aws_iam.ServicePrincipal('kafkaconnect.amazonaws.com'),
      inline_policies={
        'KafkaAccess': self._kafka_policy(),
        'SecretsAccess': self._secrets_policy(credentials_arn),
        'SchemaRegistryAccess': self._schema_registry_policy()
      }
    )

    connector = aws_kafkaconnect.CfnConnector(self, f'Connector{index}',
      connector_name=connector_name,
      capacity=aws_kafkaconnect.CfnConnector.CapacityProperty(
        provisioned_capacity=aws_kafkaconnect.CfnConnector.ProvisionedCapacityProperty(
          mcu_count=2,  # Increased for large workload
          worker_count=1  # Debezium MySQL limitation
        )
      ),
      connector_configuration={
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        
        # Database connection
        "database.hostname": db_hostname,
        "database.port": "3306",
        "database.user": f"${{secretManager:{cluster_name}-creds:username}}",
        "database.password": f"${{secretManager:{cluster_name}-creds:password}}",
        "database.server.id": str(100000 + index),
        "database.include.list": database_include_list,
        
        # Topic configuration - AGGREGATED approach
        "topic.prefix": f"cdc-{cluster_name}",
        "topic.creation.enable": "true",
        "topic.creation.default.partitions": "100",
        "topic.creation.default.replication.factor": "3",
        
        # Schema Registry (Confluent)
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": schema_registry_url,
        
        # Performance tuning for large workloads
        "snapshot.mode": "schema_only",  # Skip initial snapshot for large DBs
        "snapshot.fetch.size": "10240",
        "max.batch.size": "2048",
        "max.queue.size": "8192",
        "poll.interval.ms": "100",
        
        # Schema history
        "schema.history.internal.kafka.topic": f"schema-history-{cluster_name}",
        "schema.history.internal.kafka.bootstrap.servers": msk_bootstrap_servers,
        # ... IAM auth config ...
      },
      kafka_cluster=aws_kafkaconnect.CfnConnector.KafkaClusterProperty(
        apache_kafka_cluster=aws_kafkaconnect.CfnConnector.ApacheKafkaClusterProperty(
          bootstrap_servers=msk_bootstrap_servers,
          vpc=aws_kafkaconnect.CfnConnector.VpcProperty(
            security_groups=[...],
            subnets=[...]
          )
        )
      ),
      kafka_cluster_client_authentication=aws_kafkaconnect.CfnConnector.KafkaClusterClientAuthenticationProperty(
        authentication_type="IAM"
      ),
      kafka_cluster_encryption_in_transit=aws_kafkaconnect.CfnConnector.KafkaClusterEncryptionInTransitProperty(
        encryption_type="TLS"
      ),
      kafka_connect_version="2.7.1",
      plugins=[aws_kafkaconnect.CfnConnector.PluginProperty(
        custom_plugin=aws_kafkaconnect.CfnConnector.CustomPluginProperty(
          custom_plugin_arn=custom_plugin_arn,
          revision=1
        )
      )],
      service_execution_role_arn=connector_role.role_arn,
      log_delivery=aws_kafkaconnect.CfnConnector.LogDeliveryProperty(
        worker_log_delivery=aws_kafkaconnect.CfnConnector.WorkerLogDeliveryProperty(
          cloud_watch_logs=aws_kafkaconnect.CfnConnector.CloudWatchLogsLogDeliveryProperty(
            enabled=True,
            log_group=log_group.log_group_name
          )
        )
      ),
      worker_configuration=aws_kafkaconnect.CfnConnector.WorkerConfigurationProperty(
        revision=1,
        worker_configuration_arn=worker_config_arn
      )
    )
```

### 4. Replace Firehose with Kafka Connect S3 Sink

**Why**: Firehose stream limits (5,000) cannot support 2.86M topics.

**Solution**: Use Kafka Connect S3 Sink Connector instead of Firehose.

```
┌─────────────────────────────────────────────────────────────────┐
│              S3 SINK CONNECTOR ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  MSK Topics ──▶ S3 Sink Connector ──▶ S3 Data Lake              │
│                                                                  │
│  Benefits:                                                       │
│  - Single connector can consume from multiple topics             │
│  - Supports topic regex patterns                                 │
│  - Native Avro/Parquet support                                   │
│  - Partitioning by time, field values                           │
│  - No per-topic stream limits                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**S3 Sink Connector Configuration**:

```properties
# S3 Sink Connector for CDC data
connector.class=io.confluent.connect.s3.S3SinkConnector
tasks.max=10

# Topic selection - regex for all CDC topics
topics.regex=cdc-.*

# S3 destination
s3.bucket.name=cdc-data-lake
s3.region=us-west-2

# Partitioning strategy
partitioner.class=io.confluent.connect.storage.partitioner.TimeBasedPartitioner
path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH
partition.duration.ms=3600000
locale=en-US
timezone=UTC

# Output format
format.class=io.confluent.connect.s3.format.parquet.ParquetFormat
parquet.codec=snappy

# Flush settings
flush.size=10000
rotate.interval.ms=600000

# Schema Registry
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://schema-registry:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

---

## Cost Estimation

### Monthly Cost Comparison

| Component | POC (Current) | Enterprise Scale |
|-----------|---------------|------------------|
| MSK Serverless | ~$200/month | N/A (replaced) |
| MSK Provisioned (9× m5.4xlarge) | N/A | ~$15,000/month |
| MSK Connect (11 connectors × 2 MCU) | ~$100/month | ~$2,400/month |
| Firehose | ~$50/month | N/A (replaced) |
| S3 Sink Connector | N/A | ~$500/month |
| Schema Registry (Confluent Cloud) | N/A | ~$1,500/month |
| S3 Storage (estimated 10TB/month) | ~$10/month | ~$2,300/month |
| Data Transfer | ~$50/month | ~$5,000/month |
| **Total** | **~$410/month** | **~$26,700/month** |

---

## Implementation Phases

### Phase 1: Foundation (Weeks 1-4)
1. Deploy MSK Provisioned cluster
2. Set up Confluent Schema Registry on EKS
3. Create multi-connector CDK factory
4. Deploy S3 Sink Connector

### Phase 2: Migration (Weeks 5-8)
1. Migrate 2-3 Aurora clusters
2. Validate CDC data flow
3. Performance testing
4. Schema evolution testing

### Phase 3: Scale Out (Weeks 9-16)
1. Deploy remaining Aurora clusters
2. Implement monitoring and alerting
3. Set up cross-account S3 replication
4. Documentation and runbooks

### Phase 4: Optimization (Ongoing)
1. Cost optimization
2. Performance tuning
3. Disaster recovery testing
4. Capacity planning

---

## Key Recommendations Summary

| Area | Current POC | Recommended Change |
|------|-------------|-------------------|
| **Kafka** | MSK Serverless | MSK Provisioned (m5.4xlarge × 9+) |
| **Schema Registry** | Glue Schema Registry | Confluent Schema Registry |
| **Connectors** | 1 Debezium connector | 11 Debezium connectors (1 per cluster) |
| **S3 Delivery** | Kinesis Firehose | Kafka Connect S3 Sink |
| **Topic Strategy** | 1 topic per table | Aggregated topics per cluster |
| **Account Strategy** | Single account | Multi-account (optional) |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Debezium single-task limitation | Cannot parallelize per-cluster CDC | Accept 1 connector per cluster |
| Schema Registry availability | CDC pipeline stops | Deploy HA Schema Registry (3+ replicas) |
| MSK broker failure | Temporary data lag | 3-AZ deployment, replication factor 3 |
| Large initial snapshot | Hours/days of initial sync | Use `snapshot.mode=schema_only` |
| Binlog retention | Missing CDC events | Configure 7+ day binlog retention |

---

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `cdk_stacks/msk_provisioned.py` | CREATE | MSK Provisioned cluster |
| `cdk_stacks/kafka_connector_factory.py` | CREATE | Multi-connector factory |
| `cdk_stacks/s3_sink_connector.py` | CREATE | S3 Sink connector |
| `cdk_stacks/schema_registry_eks.py` | CREATE | Confluent Schema Registry on EKS |
| `cdk_stacks/msk_serverless.py` | DEPRECATE | Replace with provisioned |
| `cdk_stacks/firehose.py` | DEPRECATE | Replace with S3 Sink |
| `cdk_stacks/glue_schema_registry.py` | DEPRECATE | Replace with Confluent |
| `cdk.context.json` | MODIFY | Add multi-cluster configuration |
| `app.py` | MODIFY | Update stack dependencies |

---

## References

- [MSK Quotas](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html)
- [Glue Schema Registry Quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html)
- [Firehose Quotas](https://docs.aws.amazon.com/firehose/latest/dev/limits.html)
- [Debezium MySQL Connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Confluent S3 Sink Connector](https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html)
