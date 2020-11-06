#!/usr/bin/env python
from enum import Enum
import itertools
import io
import sys
import logging
from google.cloud import bigquery

from output.python.protos import bigquery_options_pb2
from google.protobuf.compiler import plugin_pb2 as plugin
from google.protobuf.descriptor_pb2 import DescriptorProto, EnumDescriptorProto

from codegen import *


# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
class BigQueryTypeEnum(Enum):
    TYPE_INT64 = 1,
    TYPE_NUMERIC = 2,
    TYPE_FLOAT64 = 3,
    TYPE_BOOL = 4,
    TYPE_STRING = 5,
    TYPE_BYTES = 6,
    TYPE_DATE = 7,
    TYPE_DATETIME = 8,
    TYPE_TIME = 9,
    TYPE_TIMESTAMP = 10,
    TYPE_ARRAY = 11,
    TYPE_STRUCT = 12,
    TYPE_GEOGRAPHY = 13


# https://googleapis.dev/python/protobuf/latest/google/protobuf/descriptor_pb2.html
# https://github.com/protocolbuffers/protobuf/blob/master/python/google/protobuf/descriptor.py
class ProtoTypeEnum(Enum):
    TYPE_DOUBLE = 1
    TYPE_FLOAT = 2
    TYPE_INT64 = 3
    TYPE_UINT64 = 4
    TYPE_INT32 = 5
    TYPE_FIXED64 = 6
    TYPE_FIXED32 = 7
    TYPE_BOOL = 8
    TYPE_STRING = 9
    TYPE_GROUP = 10
    TYPE_MESSAGE = 11
    TYPE_BYTES = 12
    TYPE_UINT32 = 13
    TYPE_ENUM = 14
    TYPE_SFIXED32 = 15
    TYPE_SFIXED64 = 16
    TYPE_SINT32 = 17
    TYPE_SINT64 = 18
    MAX_TYPE = 18


_PROTO_TO_BQ_TYPE_MAP = {
    ProtoTypeEnum.TYPE_DOUBLE: BigQueryTypeEnum.TYPE_NUMERIC,
    ProtoTypeEnum.TYPE_FLOAT: BigQueryTypeEnum.TYPE_FLOAT64,
    ProtoTypeEnum.TYPE_ENUM: BigQueryTypeEnum.TYPE_STRING,  # FUTURE: Map onto int value?
    ProtoTypeEnum.TYPE_INT64: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_SINT64: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_SFIXED64: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_UINT64: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_FIXED64: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_INT32: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_SFIXED32: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_SINT32: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_UINT32: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_FIXED32: BigQueryTypeEnum.TYPE_INT64,
    ProtoTypeEnum.TYPE_BYTES: BigQueryTypeEnum.TYPE_BYTES,
    ProtoTypeEnum.TYPE_STRING: BigQueryTypeEnum.TYPE_STRING,
    ProtoTypeEnum.TYPE_BOOL: BigQueryTypeEnum.TYPE_BOOL,
    ProtoTypeEnum.TYPE_MESSAGE: BigQueryTypeEnum.TYPE_STRUCT,
    ProtoTypeEnum.TYPE_GROUP: BigQueryTypeEnum.TYPE_STRUCT
}

_BQ_TO_TYPE_VALUE = {
    BigQueryTypeEnum.TYPE_INT64: "INTEGER",
    BigQueryTypeEnum.TYPE_NUMERIC: "NUMERIC",
    BigQueryTypeEnum.TYPE_FLOAT64: "FLOAT64",
    BigQueryTypeEnum.TYPE_BOOL: "BOOL",
    BigQueryTypeEnum.TYPE_STRING: "STRING",
    BigQueryTypeEnum.TYPE_BYTES: "BYTES",
    BigQueryTypeEnum.TYPE_DATE: "DATE",
    BigQueryTypeEnum.TYPE_DATETIME: "DATETIME",
    BigQueryTypeEnum.TYPE_TIME: "TIME",
    BigQueryTypeEnum.TYPE_TIMESTAMP: "TIMESTAMP",
    BigQueryTypeEnum.TYPE_ARRAY: "ARRAY",
    BigQueryTypeEnum.TYPE_STRUCT: "RECORD",
    BigQueryTypeEnum.TYPE_GEOGRAPHY: "GEOGRAPHY"
}


# https://expobrain.net/2015/09/13/create-a-plugin-for-google-protocol-buffer/
def _traverse(proto_file):
    """
    Function to return all the items in the given proto file.

    :param proto_file:
    :return: list containing all the items in the proto file.
    """
    def _traverse(package, items):
        for item in items:
            yield item, package

            if isinstance(item, DescriptorProto):
                for enum in item.enum_type:
                    yield enum, package

                for nested in item.nested_type:
                    nested_package = package + item.name

                    for nested_item in _traverse(nested, nested_package):
                        yield nested_item, nested_package

    return itertools.chain(
        _traverse(proto_file.package, proto_file.enum_type),
        _traverse(proto_file.package, proto_file.message_type),
    )


def _resolve_repository_fields(request: plugin.CodeGeneratorRequest, repository):
    """
    Function to resolve the fields of the items in the repository.

    :param request:
    :param repository:
    :return:
    """
    for proto_file in request.proto_file:

        # Parse request
        for item, package in _traverse(proto_file):

            if isinstance(item, DescriptorProto):

                field_type = repository.get(FieldType.to_fq_name(proto_file.package, item.name))
                fields = []
                batch_table = False

                for f in item.field:
                    fields.append(
                        Field(
                            field_index=f.number,
                            field_name=f.name,
                            field_description=f.options.Extensions[bigquery_options_pb2.description],
                            field_type=ProtoTypeEnum(f.type).name,
                            field_type_value=repository.get(f.type_name, None),
                            field_required=f.options.Extensions[bigquery_options_pb2.required],
                            is_batch_field=f.options.Extensions[bigquery_options_pb2.batch_attribute],
                            is_optional_field=f.proto3_optional
                        )
                    )

                    batch_table = batch_table or f.options.Extensions[bigquery_options_pb2.batch_attribute]

                field_type.set_batch_table(batch_table) # FUTURE: Maybe not the ideal position to set this.

                field_type.set_fields(fields)


def _generate_repository(request: plugin.CodeGeneratorRequest):
    """
    Function to generate a repository from the given request.

    :param request:
    :return: Repository
    """
    repository = {}
    root_elements = []

    # Make sure all types are present
    for proto_file in request.proto_file:

        # Parse request
        for item, package in _traverse(proto_file):

            if isinstance(item, DescriptorProto) or isinstance(item, EnumDescriptorProto):

                field_type = None

                if isinstance(item, DescriptorProto):

                    table_root = item.options.HasExtension(bigquery_options_pb2.table_root)
                    field_type = MessageFieldType(proto_file.package, proto_file.name, item.name)
                    field_type.set_table_root(table_root)

                    if table_root:
                        root_elements.append(field_type)

                elif isinstance(item, EnumDescriptorProto):

                    field_type = EnumFieldType(proto_file.package, proto_file.name, item.name)
                    field_type.set_values([EnumFieldValue(v.name, v.number) for v in item.value])

                repository[field_type.get_fq_name()] = field_type

    _resolve_repository_fields(request, repository)

    return repository, root_elements


def _contruct_bigquery_schema_rec(field: MessageFieldType, schema_fields: list):
    """
    Function to recursively generate a BigQuery schema for a message.

    :param field: MessageFieldType
    :param schema_fields: wrapper for the fields
    :return:
    """
    if field is None:
        return []

    for field in field.fields:

        if field.is_batch_field:
            schema_fields.extend(_contruct_bigquery_schema_rec(field.field_type_value, []))

        else:
            proto_type = ProtoTypeEnum._member_map_[field.field_type] # FUTURE: Resolve this in the repository
            schema_fields.append(
                bigquery.SchemaField(
                    name=field.field_name,
                    description=field.field_description,
                    mode="REQUIRED" if field.field_required else "NULLABLE",
                    field_type=_BQ_TO_TYPE_VALUE[_PROTO_TO_BQ_TYPE_MAP[proto_type]],
                    fields=_contruct_bigquery_schema_rec(field.field_type_value, [])
                )
            )

    return schema_fields


def codegen_rec(field_type: MessageFieldType, root: CodeGenImp, table_root: bool = False):

    node = CodeNopNode(table_root, field_type.batch_table)
    for field in field_type.fields:

        proto_type = ProtoTypeEnum._member_map_[field.field_type]  # FUTURE: Resolve this in the repository

        if proto_type == ProtoTypeEnum.TYPE_MESSAGE:
            if field.is_batch_field:
                batch = CodeGenGetBatchNode(field, node)
                root.add_child(batch)
                codegen_rec(field.field_type_value, batch)
            else:
                nested = CodeGenNestedNode(field)
                conditional = CodeGenConditionalNode(field)
                get = CodeGenGetFieldNode(field)
                node.add_child(nested)
                nested.add_child(conditional)
                conditional.add_child(get)
                codegen_rec(field.field_type_value, get)
        else:
            if field.is_optional_field:
                conditional = CodeGenConditionalNode(field)
                node.add_child(conditional)
                conditional.add_child(CodeGenBaseNode(field))
            else:
                node.add_child(CodeGenBaseNode(field))

    root.add_child(node)
    return root


def create_codegen_tree(root: MessageFieldType):
    class_node = CodeGenClassNode(root)

    function_node = CodeGenFunctionNode(root)
    class_node.add_child(function_node)

    codegen_rec(root, function_node, table_root=True)

    return class_node


def generate_code(request: plugin.CodeGeneratorRequest, response: plugin.CodeGeneratorResponse):
    """
    :param request: the plugin's input source
    :param response: the plugin's output sink
    :return: None
    """

    client = bigquery.Client() # JSON Serializing the schema requires the BigQuery client :(

    repo, root_elements = _generate_repository(request)
    for root_element in root_elements:
        schema_fields = []
        _contruct_bigquery_schema_rec(root_element, schema_fields)

        f = io.StringIO("")
        client.schema_to_json(schema_fields, f)
        file = response.file.add()
        file.name = root_element.name + ".json"
        file.content = f.getvalue()

        file = response.file.add()
        file.name = root_element.name + "Parser.java"
        create_codegen_tree(root_element).gen_code(file, None, None, 0)

    # Drop new repository
    f = response.file.add()
    f.name = 'repository_new.json'
    f.content = json.dumps({k: v.to_json() for k, v in repo.items()}, indent=2)


if __name__ == '__main__':
    # Read request message from stdin
    data = sys.stdin.buffer.read()

    # Parse request
    request = plugin.CodeGeneratorRequest()
    request.ParseFromString(data)

    # Create response
    response = plugin.CodeGeneratorResponse()
    response.supported_features = response.FEATURE_PROTO3_OPTIONAL

    # Generate code
    generate_code(request, response)

    # Serialise response message
    output = response.SerializeToString()

    # Write to stdout
    sys.stdout.buffer.write(output)
