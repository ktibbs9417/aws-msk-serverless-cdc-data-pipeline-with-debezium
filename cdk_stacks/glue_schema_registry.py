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