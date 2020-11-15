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
from enums import ProtoTypeEnum


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

_PROTO_TO_JAVA_TYPE_MAP = {
    ProtoTypeEnum.TYPE_DOUBLE: "Double",
    ProtoTypeEnum.TYPE_FLOAT: "Float",
    ProtoTypeEnum.TYPE_ENUM: "String",  # FUTURE: Map onto int value?
    ProtoTypeEnum.TYPE_INT64: "Integer",
    ProtoTypeEnum.TYPE_SINT64: "Long",
    ProtoTypeEnum.TYPE_SFIXED64: "Long",
    ProtoTypeEnum.TYPE_UINT64: "Long",
    ProtoTypeEnum.TYPE_FIXED64: "Long",
    ProtoTypeEnum.TYPE_INT32: "Integer",
    ProtoTypeEnum.TYPE_SFIXED32: "Integer",
    ProtoTypeEnum.TYPE_SINT32: "Integer",
    ProtoTypeEnum.TYPE_UINT32: "Integer",
    ProtoTypeEnum.TYPE_FIXED32: "Integer",
    ProtoTypeEnum.TYPE_BYTES: "Bytes",
    ProtoTypeEnum.TYPE_STRING: "String",
    ProtoTypeEnum.TYPE_BOOL: "Boolean",
    ProtoTypeEnum.TYPE_MESSAGE: None,
    ProtoTypeEnum.TYPE_GROUP: None
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
                            is_optional_field=f.proto3_optional,
                            is_repeated_field=f.label == f.LABEL_REPEATED
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


def add_codegen_node_conditionally(root: CodeGenImp, node: CodeGenImp, condition = True):
    """
    Function to conditionally add a node to the tree, if the conditional succeeds, the node is
    added and is returned by the function. If the conditional fails, the old root is returned.

    :param root:
    :param node:
    :param condition:
    :return:
    """
    if condition:
        root.add_child(node)
        return node
    return root


def codegen_rec(field_type: MessageFieldType, root: CodeGenImp, table_root: bool = False):
    """
    Recursive function to build the code generation tee.

    :param field_type:
    :param root:
    :param table_root:
    :return:
    """
    node = CodeNopNode(table_root, field_type.batch_table)
    for field in field_type.fields:

        # FUTURE: Resolve this in the repository
        proto_type = ProtoTypeEnum._member_map_[field.field_type]

        field_root = node
        field_root = add_codegen_node_conditionally(field_root, CodeGenRepeatedNode(field), field.is_repeated_field and not (table_root and field.is_batch_field))

        if proto_type == ProtoTypeEnum.TYPE_MESSAGE:
            if table_root and field.is_batch_field:
                # Batch field attached directly to the root, as to allow
                # grouping of the batch and non-batch nodes.
                field_root = add_codegen_node_conditionally(root, CodeGenGetBatchNode(field, node))
            else:
                field_root = add_codegen_node_conditionally(field_root, CodeGenNestedNode(field), not field.is_repeated_field)
                field_root = add_codegen_node_conditionally(field_root, CodeGenConditionalNode(field, field.field_required), not field.is_repeated_field)
                field_root = add_codegen_node_conditionally(field_root, CodeGenGetFieldNode(field), not field.is_repeated_field)

            # Recurse child fields
            codegen_rec(field.field_type_value, field_root)
        else:
            field_root = add_codegen_node_conditionally(field_root, CodeGenConditionalNode(field, field.field_required), field.is_optional_field)
            field_root = add_codegen_node_conditionally(field_root, CodeGenGetFieldNode(field), not field.is_repeated_field)
            field_root.add_child(CodeGenBaseNode(field))

    root.add_child(node)
    return root


def create_codegen_tree(root: MessageFieldType):
    """
    Create the code generation tee for the given MessageField.

    :param root:
    :return:
    """
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
        create_codegen_tree(root_element).gen_code(file, None, None, 0, _PROTO_TO_JAVA_TYPE_MAP)

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
