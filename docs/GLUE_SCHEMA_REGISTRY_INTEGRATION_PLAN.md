# AWS Glue Schema Registry Integration Plan

This document outlines the steps required to integrate AWS Glue Schema Registry with the existing MSK Connect Debezium CDC pipeline for schema evolution management.

## Overview

AWS Glue Schema Registry provides a centralized repository for managing and enforcing schemas for data streams. Integrating it with Debezium enables:

- **Schema Evolution**: Manage schema changes (BACKWARD, FORWARD, FULL compatibility)
- **Schema Validation**: Ensure producers and consumers use compatible schemas
- **Centralized Schema Management**: Single source of truth for all Kafka topic schemas
- **Avro/JSON Schema Support**: Use Avro for efficient serialization or JSON Schema

---

## Architecture Changes

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Aurora MySQL   │────▶│  MSK Connect     │────▶│  MSK Serverless     │
│  (CDC Source)   │     │  (Debezium +     │     │  (Kafka Topics)     │
└─────────────────┘     │  Glue SerDe)     │     └──────────┬──────────┘
                        └────────┬─────────┘                │
                                 │                          │
                                 ▼                          ▼
                        ┌────────────────────┐     ┌─────────────────────┐
                        │  Glue Schema       │     │  Kinesis Firehose   │
                        │  Registry          │     │  (with Glue SerDe)  │
                        │  - retail-trans    │     └──────────┬──────────┘
                        │    schema          │                │
                        └────────────────────┘                ▼
                                                     ┌─────────────────────┐
                                                     │  Amazon S3          │
                                                     │  (Avro/Parquet)     │
                                                     └─────────────────────┘
```

---

## Implementation Steps

### Step 1: Create New CDK Stack for Glue Schema Registry

Create a new file: `cdk_stacks/glue_schema_registry.py`

```python
#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import aws_cdk as cdk

from aws_cdk import (
  Stack,
  aws_glue as glue,
  aws_iam,
)
from constructs import Construct


class GlueSchemaRegistryStack(Stack):

  def __init__(self, scope: Construct, construct_id: str, msk_cluster_name: str, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    # Create Glue Schema Registry
    self.registry = glue.CfnRegistry(self, 'CDCSchemaRegistry',
      name=f'{msk_cluster_name}-schema-registry',
      description='Schema Registry for CDC events from Debezium MySQL connector'
    )

    # Create initial schema for retail_trans table (Avro format)
    # This schema will evolve as the source table changes
    retail_trans_avro_schema = '''{
      "type": "record",
      "name": "Envelope",
      "namespace": "retail_server.testdb.retail_trans",
      "fields": [
        {
          "name": "before",
          "type": ["null", {
            "type": "record",
            "name": "Value",
            "fields": [
              {"name": "trans_id", "type": "long"},
              {"name": "customer_id", "type": "string"},
              {"name": "event", "type": "string"},
              {"name": "sku", "type": "string"},
              {"name": "amount", "type": "int"},
              {"name": "device", "type": "string"},
              {"name": "trans_datetime", "type": {"type": "long", "logicalType": "timestamp-micros"}}
            ]
          }],
          "default": null
        },
        {
          "name": "after",
          "type": ["null", "Value"],
          "default": null
        },
        {
          "name": "source",
          "type": {
            "type": "record",
            "name": "Source",
            "fields": [
              {"name": "version", "type": "string"},
              {"name": "connector", "type": "string"},
              {"name": "name", "type": "string"},
              {"name": "ts_ms", "type": "long"},
              {"name": "db", "type": "string"},
              {"name": "table", "type": ["null", "string"], "default": null},
              {"name": "server_id", "type": "long"},
              {"name": "file", "type": ["null", "string"], "default": null},
              {"name": "pos", "type": "long"},
              {"name": "row", "type": "int"}
            ]
          }
        },
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": null}
      ]
    }'''

    self.schema = glue.CfnSchema(self, 'RetailTransSchema',
      compatibility='NONE',  # IMPORTANT: Use NONE for Debezium CDC - dynamic schemas are not backward compatible
      data_format='AVRO',  # Options: AVRO, JSON, PROTOBUF
      name='retail-trans-cdc-schema',
      schema_definition=retail_trans_avro_schema,
      registry=glue.CfnSchema.RegistryProperty(
        arn=self.registry.attr_arn
      ),
      description='Debezium CDC schema for retail_trans table'
    )

    self.registry_name = self.registry.name
    self.registry_arn = self.registry.attr_arn
    self.schema_arn = self.schema.attr_arn

    cdk.CfnOutput(self, 'GlueRegistryName', value=self.registry.name,
      export_name=f'{self.stack_name}-RegistryName')
    cdk.CfnOutput(self, 'GlueRegistryArn', value=self.registry.attr_arn,
      export_name=f'{self.stack_name}-RegistryArn')
    cdk.CfnOutput(self, 'GlueSchemaArn', value=self.schema.attr_arn,
      export_name=f'{self.stack_name}-SchemaArn')
```

---

### Step 2: Update `cdk_stacks/__init__.py`

Add the new stack export:

```python
# Add this import
from .glue_schema_registry import GlueSchemaRegistryStack

```

---

### Step 3: Update `app.py`

Add the Glue Schema Registry stack to the application:

```python
# Add import
from cdk_stacks import (
  # ... existing imports ...
  GlueSchemaRegistryStack
)

# Add after msk_stack creation (around line 40)
glue_registry_stack = GlueSchemaRegistryStack(app, 'GlueSchemaRegistryStack',
  msk_stack.msk_cluster_name,
  env=AWS_ENV
)
glue_registry_stack.add_dependency(msk_stack)

# Update msk_connector_stack to depend on glue_registry_stack
# and pass registry information
msk_connector_stack = KafkaConnectorStack(app, 'KafkaConnectorStack',
  vpc_stack.vpc,
  aurora_mysql_stack.db_hostname,
  aurora_mysql_stack.sg_mysql_client,
  aurora_mysql_stack.rds_credentials,
  msk_stack.msk_cluster_name,
  msk_stack.msk_cluster_vpc_configs,
  glue_registry_stack.registry_name,  # NEW PARAMETER
  env=AWS_ENV
)
msk_connector_stack.add_dependency(glue_registry_stack)  # Updated dependency
```

---

### Step 4: Update `cdk_stacks/kafka_connector.py`

#### 4.1 Update Constructor Signature

```python
def __init__(self, scope: Construct, construct_id: str,
  vpc, db_hostname, sg_rds_client, rds_credentials,
  msk_cluster_name, msk_cluster_vpc_configs,
  glue_registry_name,  # NEW PARAMETER
  **kwargs) -> None:
```

#### 4.2 Add Glue Schema Registry IAM Permissions

Add this policy document after the existing `secretmanager_readonly_access_policy_doc` within `/cdk_stacks/kafka_connector.py`:

```python
# Glue Schema Registry access policy
glue_schema_registry_policy_doc = aws_iam.PolicyDocument()
glue_schema_registry_policy_doc.add_statements(aws_iam.PolicyStatement(**{
  "effect": aws_iam.Effect.ALLOW,
  "actions": [
    "glue:GetRegistry",
    "glue:ListRegistries",
    "glue:GetSchema",
    "glue:ListSchemas",
    "glue:GetSchemaVersion",
    "glue:ListSchemaVersions",
    "glue:GetSchemaByDefinition",
    "glue:GetSchemaVersionsDiff",
    "glue:CheckSchemaVersionValidity",
    "glue:RegisterSchemaVersion",
    "glue:CreateSchema",
    "glue:UpdateSchema",
    "glue:TagResource",
    "glue:GetTags"
  ],
  "resources": [
    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:registry/{glue_registry_name}",
    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:schema/{glue_registry_name}/*"
  ]
}))
```

#### 4.3 Update IAM Role with Glue Policy

```python
msk_connector_execution_role = aws_iam.Role(self, 'MSKConnectorExecutionRole',
  role_name=f'MSKConnectorExecutionRole-{msk_cluster_name}',
  assumed_by=aws_iam.ServicePrincipal('kafkaconnect.amazonaws.com'),
  path='/',
  inline_policies={
    'KafkaClusterAccessPolicy': kafka_cluster_access_policy_doc,
    'SecretsManagerReadOnlyAccessPolicy': secretmanager_readonly_access_policy_doc,
    'GlueSchemaRegistryPolicy': glue_schema_registry_policy_doc  # NEW POLICY
  }
)
```

#### 4.4 Update Connector Configuration

Replace the existing `connector_configuration` located within `within /cdk_stacks/kafka_connector.py `with Glue Schema Registry converters:

```python
msk_connector = aws_kafkaconnect.CfnConnector(self, "KafkaCfnConnector",
  # ... existing capacity config ...
  connector_configuration={
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": msk_connector_configuration['tasks.max'],

    # Database connection settings (unchanged)
    "database.hostname": db_hostname,
    "database.port": "3306",
    "database.user": f"${{secretManager:{rds_secret_name}:username}}",
    "database.password": f"${{secretManager:{rds_secret_name}:password}}",
    "database.server.id": "123456",
    "database.include.list": msk_connector_configuration['database.include.list'],

    # Topic settings (unchanged)
    "topic.prefix": msk_connector_configuration['topic.prefix'],
    "topic.creation.enable": "true",
    "topic.creation.default.partitions": msk_connector_configuration['topic.creation.default.partitions'],
    "topic.creation.default.replication.factor": msk_connector_configuration['topic.creation.default.replication.factor'],

    "include.schema.changes": msk_connector_configuration['include.schema.changes'],

    # Schema history settings (unchanged)
    "schema.history.internal.kafka.topic": msk_connector_configuration['schema.history.internal.kafka.topic'],
    "schema.history.internal.kafka.bootstrap.servers": kafka_booststrap_servers,
    "schema.history.internal.consumer.security.protocol": "SASL_SSL",
    "schema.history.internal.consumer.sasl.mechanism": "AWS_MSK_IAM",
    "schema.history.internal.consumer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "schema.history.internal.consumer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "schema.history.internal.producer.security.protocol": "SASL_SSL",
    "schema.history.internal.producer.sasl.mechanism": "AWS_MSK_IAM",
    "schema.history.internal.producer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "schema.history.internal.producer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",

    # ========== NEW: Glue Schema Registry Configuration ==========
    # Key converter - use Avro with Glue Schema Registry
    "key.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
    "key.converter.region": cdk.Aws.REGION,
    "key.converter.schemaAutoRegistrationEnabled": "true",
    "key.converter.avroRecordType": "GENERIC_RECORD",
    "key.converter.registry.name": glue_registry_name,

    # Value converter - use Avro with Glue Schema Registry
    "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
    "value.converter.region": cdk.Aws.REGION,
    "value.converter.schemaAutoRegistrationEnabled": "true",
    "value.converter.avroRecordType": "GENERIC_RECORD",
    "value.converter.registry.name": glue_registry_name,
    "value.converter.compatibility": "NONE",  # IMPORTANT: Use NONE for Debezium CDC
  },
  # ... rest of connector config unchanged ...
)
```

---

### Step 5: Update Worker Configuration

Update `msk-connector-worker-config.txt`:

```properties
# Key converter - Glue Schema Registry Avro
key.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter
key.converter.region=us-west-2
key.converter.schemaAutoRegistrationEnabled=true
key.converter.avroRecordType=GENERIC_RECORD

# Value converter - Glue Schema Registry Avro
value.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter
value.converter.region=us-west-2
value.converter.schemaAutoRegistrationEnabled=true
value.converter.avroRecordType=GENERIC_RECORD
value.converter.compatibility=NONE

# Secrets Manager config provider (unchanged)
# CRITICAL: The custom plugin MUST include jcustenborder-kafka-config-provider-aws JARs
config.providers.secretManager.class=com.github.jcustenborder.kafka.config.aws.SecretsManagerConfigProvider
config.providers=secretManager
config.providers.secretManager.param.aws.region=us-west-2
```

> **Note**: The `value.converter.compatibility=NONE` setting is critical for Debezium CDC.
> Debezium generates multiple schema types dynamically that are not backward compatible.

After updating, regenerate the base64 file:

```bash
base64 -i msk-connector-worker-config.txt -o msk-connector-worker-config.b64
```
Create the worker configuration
```bash
aws kafkaconnect create-worker-configuration \
    --name debezium-worker-glue \
    --properties-file-content file://msk-connector-worker-config.b64
```

---

### Step 6: Update Custom Plugin with Glue SerDe Library

The Debezium connector plugin needs the AWS Glue Schema Registry SerDe library.

> **⚠️ CRITICAL: Include AWS Secrets Manager Config Provider JARs**
>
> The custom plugin MUST include the `jcustenborder-kafka-config-provider-aws` library and its dependencies.
> Without these JARs, the connector will fail with:
> ```
> ClassNotFoundException: com.github.jcustenborder.kafka.config.aws.SecretsManagerConfigProvider
> ```
> This is required because the worker configuration uses `${secretManager:...}` syntax to retrieve
> database credentials from AWS Secrets Manager.

#### 6.1 Download Required JARs

```bash
# Create a directory for the updated plugin
mkdir -p debezium-connector-mysql-glue/

# Copy existing Debezium JARs
cp -r debezium-connector-mysql/debezium-connector-mysql/*.jar debezium-connector-mysql-glue/

# ========== CRITICAL: Copy AWS Secrets Manager Config Provider JARs ==========
# These are REQUIRED for the ${secretManager:...} syntax in connector configuration
cp -r debezium-connector-mysql/jcustenborder-kafka-config-provider-aws-0.1.2/lib/* debezium-connector-mysql-glue/

# Download AWS Glue Schema Registry SerDe
# Latest version: https://github.com/awslabs/aws-glue-schema-registry
curl -L -o debezium-connector-mysql-glue/aws-glue-schema-registry-serde-1.1.19.jar \
  https://repo1.maven.org/maven2/software/amazon/glue/schema-registry-serde/1.1.19/schema-registry-serde-1.1.19.jar

# Download Kafka Connect Avro Converter
curl -L -o debezium-connector-mysql-glue/aws-glue-schema-registry-kafkaconnect-converter-1.1.19.jar \
  https://repo1.maven.org/maven2/software/amazon/glue/schema-registry-kafkaconnect-converter/1.1.19/schema-registry-kafkaconnect-converter-1.1.19.jar

# Download Avro library (required dependency)
curl -L -o debezium-connector-mysql-glue/avro-1.11.3.jar \
  https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar
```

#### 6.2 Verify Plugin Contents

Before creating the ZIP, verify all required JARs are present:

```bash
ls -la debezium-connector-mysql-glue/

# Expected JARs (minimum required):
# - debezium-connector-mysql-*.jar (Debezium connector)
# - debezium-core-*.jar
# - debezium-api-*.jar
# - debezium-ddl-parser-*.jar
# - debezium-storage-*.jar
# - mysql-connector-j-*.jar (MySQL JDBC driver)
# - mysql-binlog-connector-java-*.jar
# - kafka-config-provider-aws-*.jar (Secrets Manager Config Provider - CRITICAL)
# - guava-*.jar (Required by Secrets Manager Config Provider - CRITICAL)
# - aws-java-sdk-secretsmanager-*.jar
# - aws-java-sdk-core-*.jar
# - aws-glue-schema-registry-*.jar (Glue SerDe)
# - avro-*.jar
```

#### 6.3 Create New Plugin ZIP

```bash
cd debezium-connector-mysql-glue
zip -r ../debezium-connector-mysql-glue-v2.4.0.zip .
cd ..
```

#### 6.3 Upload to S3 and Create Custom Plugin

```bash
# Upload new plugin to S3
aws s3 cp debezium-connector-mysql-glue-v2.4.0.zip \
  s3://demo-msk-pipeline-321/debezium-connector/debezium-connector-mysql-glue-v2.4.0.zip

# Create new custom plugin
aws kafkaconnect create-custom-plugin \
  --name debezium-connector-mysql-glue-v2-4-0 \
  --content-type ZIP \
  --location "s3Location={bucketArn=arn:aws:s3:::demo-msk-pipeline-321,fileKey=debezium-connector/debezium-connector-mysql-glue-v2.4.0.zip}"
```

#### 6.4 Update `debezium-source-custom-plugin.json`

```json
{
  "name": "debezium-connector-mysql-glue-v2-4-0",
  "contentType": "ZIP",
  "location": {
    "s3Location": {
      "bucketArn": "arn:aws:s3:::demo-msk-pipeline-321",
      "fileKey": "debezium-connector/debezium-connector-mysql-glue-v2.4.0.zip"
    }
  }
}
```

---

### Step 7: Update `cdk.context.json`

Add Glue Schema Registry configuration:

```json
{
  "vpc_name": "default",
  "db_cluster_name": "retail",
  "msk_cluster_name": "retail-trans",
  "msk_connector_worker_configuration_name": "debezium-worker-glue",
  "msk_connector_custom_plugin_name": "debezium-connector-mysql-glue-v2-4-0",
  "msk_connector_name": "retail-changes",
  "msk_connector_configuration": {
    "tasks.max": "1",
    "database.include.list": "testdb",
    "topic.prefix": "retail-server",
    "topic.creation.default.partitions": "3",
    "topic.creation.default.replication.factor": "2",
    "include.schema.changes": "true",
    "schema.history.internal.kafka.topic": "schema-changes.testdb"
  },
  "glue_schema_registry": {
    "registry_name": "retail-trans-schema-registry",
    "compatibility_mode": "NONE",
    "data_format": "AVRO"
  },
  "firehose": {
    "buffering_hints": {
      "intervalInSeconds": 300,
      "sizeInMBs": 100
    },
    "topic_name": "retail-server.testdb.retail_trans"
  }
}
```

---

### Step 8: Update Firehose Stack (Optional - for Avro to Parquet conversion)

If you want Firehose to deserialize Avro and convert to Parquet, update `cdk_stacks/firehose.py`:

#### 8.1 Add Glue IAM Permissions

```python
# Add to firehose_role_policy_doc
firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(**{
  "effect": aws_iam.Effect.ALLOW,
  "actions": [
    "glue:GetRegistry",
    "glue:GetSchema",
    "glue:GetSchemaVersion"
  ],
  "resources": [
    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:registry/*",
    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:schema/*"
  ]
}))
```

#### 8.2 Enable Data Format Conversion (Avro to Parquet)

```python
extended_s3_dest_config = aws_kinesisfirehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
  bucket_arn=s3_bucket.bucket_arn,
  role_arn=firehose_role.role_arn,
  buffering_hints=firehose_buffering_hints,
  cloud_watch_logging_options={
    "enabled": True,
    "logGroupName": firehose_log_group_name,
    "logStreamName": "DestinationDelivery"
  },
  compression_format="UNCOMPRESSED",
  # Enable data format conversion
  data_format_conversion_configuration={
    "enabled": True,
    "inputFormatConfiguration": {
      "deserializer": {
        "openXJsonSerDe": {}  # For JSON, or use HiveJsonSerDe for Avro
      }
    },
    "outputFormatConfiguration": {
      "serializer": {
        "parquetSerDe": {
          "compression": "SNAPPY"
        }
      }
    },
    "schemaConfiguration": {
      "catalogId": cdk.Aws.ACCOUNT_ID,
      "databaseName": "cdc_database",  # Create this Glue database
      "tableName": "retail_trans",      # Create this Glue table
      "region": cdk.Aws.REGION,
      "roleArn": firehose_role.role_arn
    }
  },
  error_output_prefix="error/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}",
  prefix="parquet-data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
)
```

---

## Updated Deployment Order

```bash
# Step 1: VPC and Aurora MySQL (unchanged)
cdk deploy MSKServerlessToS3VpcStack AuroraMySQLAsDataSourceStack

# Step 2: MSK Serverless (unchanged)
cdk deploy MSKServerlessStack MSKClusterPolicy

# Step 3: Glue Schema Registry (NEW)
cdk deploy GlueSchemaRegistryStack

# Step 4: Bastion Host (unchanged)
cdk deploy BastionHost

# Step 5: Create updated custom plugin and worker configuration
# (See Step 6 above for commands)

# Step 6: MSK Connect with Glue SerDe
cdk deploy KafkaConnectorStack

# Step 7: Firehose to S3
cdk deploy S3AsFirehoseDestinationStack FirehosefromMSKtoS3Stack
```

---

## Schema Evolution Modes

> **⚠️ IMPORTANT: Use `NONE` Compatibility for Debezium CDC**
>
> Debezium dynamically generates multiple schema types (e.g., `SchemaChangeValue`, `Envelope`, etc.)
> that may not be backward compatible with each other. Using `BACKWARD` or other strict compatibility
> modes will cause schema registration failures with errors like:
> ```
> AWSSchemaRegistryException: Schema Found but status is FAILURE
> ```
>
> **Recommendation**: Use `NONE` compatibility mode for Debezium CDC pipelines, or configure
> separate schemas per topic with appropriate compatibility modes.

Choose the appropriate compatibility mode in `cdk.context.json`:

| Mode | Description | Use Case |
|------|-------------|----------|
| `NONE` | No compatibility checking | **Recommended for Debezium CDC** |
| `BACKWARD` | New schema can read old data | Stable schemas only, NOT for Debezium |
| `BACKWARD_ALL` | New schema can read all old versions | Strict backward compatibility |
| `FORWARD` | Old schema can read new data | When consumers update before producers |
| `FORWARD_ALL` | All old schemas can read new data | Strict forward compatibility |
| `FULL` | Both backward and forward compatible | Maximum flexibility |
| `FULL_ALL` | All versions are fully compatible | Strictest mode |

---

## Testing Schema Evolution

After deployment, test schema evolution:

```bash
# 1. Add a new nullable column to MySQL
mysql> ALTER TABLE retail_trans ADD COLUMN discount DECIMAL(5,2) DEFAULT NULL;

# 2. Debezium will detect the change and register a new schema version
# 3. Check the new schema version in Glue Console or via CLI:
aws glue get-schema-version \
  --schema-id "RegistryName=retail-trans-schema-registry,SchemaName=retail-trans-cdc-schema" \
  --schema-version-number LatestVersion
```

---

## Files to Modify Summary

| File | Action | Description |
|------|--------|-------------|
| `cdk_stacks/glue_schema_registry.py` | CREATE | New stack for Glue Schema Registry |
| `cdk_stacks/__init__.py` | MODIFY | Export new stack |
| `app.py` | MODIFY | Add Glue stack, update dependencies |
| `cdk_stacks/kafka_connector.py` | MODIFY | Add Glue IAM, update converter config |
| `cdk.context.json` | MODIFY | Add Glue config, update plugin name |
| `msk-connector-worker-config.txt` | MODIFY | Update converters to Glue SerDe |
| `msk-connector-worker-config.b64` | REGENERATE | Base64 encode updated config |
| `debezium-source-custom-plugin.json` | MODIFY | Point to new plugin with Glue JARs |
| `cdk_stacks/firehose.py` | MODIFY (optional) | Add Glue IAM for Parquet conversion |
| `requirements.txt` | NO CHANGE | No new Python dependencies needed |

---

## Required JAR Files for Custom Plugin

Download and include these in your custom plugin ZIP:

### Glue Schema Registry JARs
1. `schema-registry-serde-1.1.19.jar` - AWS Glue Schema Registry SerDe
2. `schema-registry-kafkaconnect-converter-1.1.19.jar` - Kafka Connect converter
3. `avro-1.11.3.jar` - Apache Avro library

### AWS Secrets Manager Config Provider JARs (CRITICAL)
These are required for the `${secretManager:...}` syntax in connector configuration:

1. `kafka-config-provider-aws-0.1.2.jar` - Main config provider
2. `guava-31.1-jre.jar` - Required dependency (download separately)
3. `aws-java-sdk-secretsmanager-*.jar` - AWS SDK for Secrets Manager
4. `aws-java-sdk-core-*.jar` - AWS SDK Core
5. `aws-java-sdk-sts-*.jar` - AWS SDK STS
6. Additional transitive dependencies from the `jcustenborder-kafka-config-provider-aws` library

### Debezium MySQL Connector JARs
1. `debezium-connector-mysql-*.jar`
2. `debezium-core-*.jar`
3. `debezium-api-*.jar`
4. `debezium-ddl-parser-*.jar`
5. `debezium-storage-kafka-*.jar`
6. `debezium-storage-file-*.jar`
7. `mysql-connector-j-*.jar` - MySQL JDBC driver
8. `mysql-binlog-connector-java-*.jar`
9. `antlr4-runtime-*.jar`
10. `zstd-jni-*.jar`

Maven coordinates for Glue Schema Registry:
```xml
<dependency>
  <groupId>software.amazon.glue</groupId>
  <artifactId>schema-registry-serde</artifactId>
  <version>1.1.19</version>
</dependency>
<dependency>
  <groupId>software.amazon.glue</groupId>
  <artifactId>schema-registry-kafkaconnect-converter</artifactId>
  <version>1.1.19</version>
</dependency>
```

> **⚠️ Important:** The easiest way to get all required JARs is to:
> 1. Start with the original `debezium-connector-mysql` directory (includes Secrets Manager Config Provider)
> 2. Add the Glue Schema Registry JARs on top
> 3. Verify all JARs are present before creating the ZIP

---

## References

- [AWS Glue Schema Registry Documentation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
- [AWS Glue Schema Registry GitHub](https://github.com/awslabs/aws-glue-schema-registry)
- [Debezium Converters Documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html)
- [MSK Connect Custom Plugins](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html)

---

## Troubleshooting

### Error: ClassNotFoundException for SecretsManagerConfigProvider

**Symptom:**
```
ClassNotFoundException: com.github.jcustenborder.kafka.config.aws.SecretsManagerConfigProvider
```

**Cause:** The custom plugin ZIP is missing the AWS Secrets Manager Config Provider JARs.

**Solution:**
1. Download the `jcustenborder-kafka-config-provider-aws` library from [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-config-provider-aws)
2. Copy all JARs from the `lib/` directory to your plugin directory
3. **Important:** Also download and include `guava-31.1-jre.jar` from [Maven Central](https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/)
4. Recreate the ZIP and upload to S3
5. Create a new custom plugin or update the existing one

```bash
# Copy Secrets Manager Config Provider JARs
cp -r debezium-connector-mysql/jcustenborder-kafka-config-provider-aws-0.1.2/lib/* debezium-connector-mysql-glue/

# Verify guava JAR is present (required dependency)
ls debezium-connector-mysql-glue/guava*.jar

# Recreate ZIP
cd debezium-connector-mysql-glue && zip -r ../debezium-connector-mysql-glue-v2.4.0.zip . && cd ..
```

---

### Error: Schema Found but status is FAILURE

**Symptom:**
```
AWSSchemaRegistryException: Schema Found but status is FAILURE
AWSSchemaRegistryException: Failed to get schemaVersionId by schema definition for schema name = retail-server
```

**Cause:** Schema compatibility check failed. Debezium generates multiple schema types (e.g., `SchemaChangeValue`, `Envelope`) that are not backward compatible with each other.

**Solution:**
1. Delete the failed schema version:
   ```bash
   aws glue delete-schema-versions \
     --schema-id SchemaName=<schema-name>,RegistryName=<registry-name> \
     --versions "<version-number>" \
     --region us-west-2
   ```

2. Update schema compatibility to `NONE`:
   ```bash
   # Set checkpoint on existing version first
   aws glue put-schema-version-metadata \
     --schema-id SchemaName=<schema-name>,RegistryName=<registry-name> \
     --schema-version-number VersionNumber=1 \
     --metadata-key-value MetadataKey=checkpoint,MetadataValue=true \
     --region us-west-2

   # Update compatibility
   aws glue update-schema \
     --schema-id SchemaName=<schema-name>,RegistryName=<registry-name> \
     --compatibility NONE \
     --schema-version-number VersionNumber=1 \
     --region us-west-2
   ```

3. Restart the connector (destroy and redeploy KafkaConnectorStack)

**Prevention:** Always use `compatibility=NONE` for Debezium CDC pipelines in:
- `cdk_stacks/glue_schema_registry.py`
- `msk-connector-worker-config.txt` (`value.converter.compatibility=NONE`)
- Connector configuration (`value.converter.compatibility`)

---

### Error: Only a single connector task may be started

**Symptom:**
```
IllegalArgumentException: Only a single connector task may be started
```

**Cause:** MSK Connect auto-scaling is configured with `max_worker_count > 1`, but Debezium MySQL connector only supports `tasks.max=1`.

**Solution:** Use provisioned capacity with a single worker instead of auto-scaling:

```python
# In kafka_connector.py
capacity=aws_kafkaconnect.CfnConnector.CapacityProperty(
    provisioned_capacity=aws_kafkaconnect.CfnConnector.ProvisionedCapacityProperty(
        mcu_count=1,
        worker_count=1
    )
)
```

---

### Verifying Schema Registry Status

Check schema versions and their status:

```bash
# List schemas in registry
aws glue list-schemas \
  --registry-id RegistryName=retail-trans-schema-registry \
  --region us-west-2

# List schema versions
aws glue list-schema-versions \
  --schema-id SchemaName=<schema-name>,RegistryName=retail-trans-schema-registry \
  --region us-west-2

# Get schema details
aws glue get-schema \
  --schema-id SchemaName=<schema-name>,RegistryName=retail-trans-schema-registry \
  --region us-west-2
```
