#!/usr/bin/env python
from enum import Enum
import itertools
import json
import io
import sys
import logging
import re
from google.cloud import bigquery

from output.python.protos import bigquery_options_pb2
from google.protobuf.compiler import plugin_pb2 as plugin
from google.protobuf.descriptor_pb2 import DescriptorProto, EnumDescriptorProto

from fields import Field, MessageFieldType, EnumFieldType, EnumFieldValue, FieldType


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
    ProtoTypeEnum.TYPE_ENUM: BigQueryTypeEnum.TYPE_STRING,  # FUTURE: Map onto int value
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


def _resolve_repository_fields(request, repository):
    for proto_file in request.proto_file:

        # Parse request
        for item, package in _traverse(proto_file):

            if isinstance(item, DescriptorProto):

                field = repository.get(FieldType.to_fq_name(proto_file.package, item.name))
                fields = []
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

                field.set_fields(fields)


def _generate_repository(request):

    repository = {}
    root_elements = []

    # Make sure all types are present
    for proto_file in request.proto_file:

        # Parse request
        for item, package in _traverse(proto_file):

            if isinstance(item, DescriptorProto) or isinstance(item, EnumDescriptorProto):

                field = None

                if isinstance(item, DescriptorProto):

                    table_root = item.options.HasExtension(bigquery_options_pb2.table_root)
                    field = MessageFieldType(proto_file.package, proto_file.name, item.name)
                    field.set_table_root(table_root)

                    if table_root:
                        root_elements.append(field)

                elif isinstance(item, EnumDescriptorProto):

                    field = EnumFieldType(proto_file.package, proto_file.name, item.name)
                    field.set_values([EnumFieldValue(v.name, v.number) for v in item.value])

                repository[field.get_fq_name()] = field

    _resolve_repository_fields(request, repository)

    return repository, root_elements


def _contruct_bigquery_schema_rec(field: MessageFieldType, schema_fields: list):

    if field is None:
        return []

    for field in field.fields:

        if field.is_batch_field:
            schema_fields.extend(_contruct_bigquery_schema_rec(field.field_type_value, []))

        else:
            proto_type = ProtoTypeEnum._member_map_[field.field_type]
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


def underscore_to_camelcase(s):
    """
    Transform given string in undercase format to string in upperCamelCase format.

    :param s: string to format
    :return: CamelCase string
    """
    return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)


def to_variable(s):
    """
    Generate a variable name from the given string

    :param s: the string
    :return: variable name for the string
    """
    if len(s) > 0:
        return s[0].lower() + underscore_to_camelcase(s[1:])
    return s


def indent(str, depth):
    """
    Indent given string to the given depth.

    :param str: string to indent
    :param depth: depth to indent to
    :return: indented string
    """
    return "\t" * depth + str + "\n"


def parser_handle_base_field(depth, root_var, field, proto_path, file):
    """
    Generate code for base proto message attribute

    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param field: field being handled by the parser
    :param proto_path: path in proto element to retrieve final value
    :param file: output file to write code to
    :return:
    """
    get_cell = underscore_to_camelcase(f"{proto_path}.get_{field['fieldName']}()")
    file.content += indent(f"{root_var}.set(\"{field['fieldName']}\", {get_cell});", depth)


def parser_handle_optional_field(repository, depth, root_var, field, proto_path, file):
    """
    Generate code for an optional proto message attribute

    :param repository: the repository of message types and enums
    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param field: field being handled by the parser
    :param proto_path: path in proto element to retrieve final value
    :param file: output file to write code to
    :return:
    """
    has_cell = underscore_to_camelcase(f"{proto_path}.has_{field['fieldName']}()")

    file.content += indent(f"if({has_cell}) {{", depth)
    parser_handle_fields(repository, depth + 1, root_var, field, proto_path, file)

    if not field['fieldRequired']:
        file.content += indent("} \n", depth)
    else:
        file.content += indent("} else {", depth)
        file.content += indent("throw new Exception();", depth + 1)
        file.content += indent("} \n", depth)


def parser_handle_batch_field(repository, depth, root_var, field, proto_path, file):
    """
    Generate code for a batch attribute

    :param repository: the repository of message types and enums
    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param field: field being handled by the parser
    :param proto_path: path in proto element to retrieve final value
    :param file: output file to write code to
    :return:
    """
    field_type = repository.get(field["fieldTypeValue"])
    has_cell = underscore_to_camelcase(f"has_{field_type['name']}()")
    get_cell = underscore_to_camelcase(f"get_{field_type['name']}()")
    new_path = f"{proto_path}.{get_cell}"

    file.content += "\n"
    file.content += indent(f"// {field_type['name']}", depth)
    file.content += indent(f"if({proto_path}.{has_cell}) {{", depth)

    parser_handle_fields(repository, depth + 1, root_var, field, new_path, file)

    file.content += indent("} \n", depth)
    #parser_handle_optional_field(repository, depth + 1, root_var, field, proto_path, file)


def parser_handle_nested_field(repository, depth, root_var, field, proto_path, file):
    """
    Generate code for a nested proto message

    :param repository: the repository of message types and enums
    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param field: field being handled by the parser
    :param proto_path: path in proto element to retrieve final value
    :param file: output file to write code to
    :return:
    """
    field_name = field["fieldName"]
    field_type = repository.get(field["fieldTypeValue"])
    cell_name = f"{field_name}Cell"
    get_cell = underscore_to_camelcase(f"get_{field_type['name']}()")
    new_path = f"{proto_path}.{get_cell}"

    file.content += "\n"
    file.content += indent(f"// {field_type['name']}", depth)
    file.content += indent(f"TableCell {cell_name} = new TableCell();", depth)

    parser_handle_optional_field(repository, depth, cell_name, field, new_path, file)

    file.content += indent(f"{root_var}.set(\"{field_name}\", {cell_name});", depth)


def parser_handle_fields(repository, depth, root_var, field, proto_path, file):
    """
    Generate code for a specific proto message

    :param repository: the repository of message types and enums
    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param fields: fields of the message currently being handled
    :param proto_path: path in proto element to retrieve final value
    :param file: output file to write code to
    :return:
    """
    field_type = repository.get(field["fieldTypeValue"])

    if field_type is None:  # Base fields are not found in repo
        return parser_handle_base_field(depth, root_var, field, proto_path, file)
    else:
        for f in field_type["fields"]:
            proto_type = ProtoTypeEnum._member_map_[f["fieldType"]]

            if proto_type == ProtoTypeEnum.TYPE_MESSAGE:
                parser_handle_nested_field(repository, depth, root_var, f, proto_path, file)
            elif f["isOptionalField"]:
                parser_handle_optional_field(repository, depth, root_var, f, proto_path, file)
            else:
                parser_handle_base_field(depth, root_var, f, proto_path, file)


def parser_handle_batch_table(repository, outer_name, depth, root_var, field, batch_field, file):
    """
    Code generation for table with batching enabled.

    :param repository: the repository of message types and enums
    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param field: field being handled by the parser
    :param batch_field: reference to the batch field in the repository
    :param batch_attribute: name of the batch attribute
    :param file: output file to write code to
    :return:
    """
    batch_field_name = batch_field["fieldName"]
    batch_field_type = repository.get(batch_field["fieldTypeValue"])
    root_var_name = "row"
    list_name = underscore_to_camelcase(f"{batch_field_name}_rows")
    get_list = underscore_to_camelcase(f"{root_var}.get_{batch_field_name}_list()")
    item = f"{to_variable(batch_field_type['name'])}"
    file.content += indent(f"List<TableRow> {list_name} = new LinkedList<>();", depth)
    file.content += indent("", depth)
    file.content += indent(f"for({outer_name}.{batch_field_type['name']} {item}: {get_list}) {{", depth)
    file.content += indent("", depth)
    file.content += indent(f"TableRow {root_var_name} = new TableRow();", depth + 1)

    for f in field["fields"]:
        if f["isBatchField"]:
            parser_handle_fields(repository, depth + 1, root_var_name, batch_field, item, file)
        else:
            parser_handle_batch_field(repository, depth + 1, root_var_name, f, root_var, file)

    file.content += indent(f"{list_name}.add({root_var_name});", depth + 1)
    file.content += indent("}\n", depth)
    file.content += indent(f"return {list_name};", depth)


def parser_handle_table(repository, depth, root_var, field, file):
    """
    Code generation for a table where batching is disabled.

    :param repository: the repository of message types and enums
    :param depth: depth for formatting purposes
    :param root_var: root element to add contents to (either table row or cell)
    :param field: field being handled by the parser
    :param file: output file to write code to
    :return:
    """
    root_var_name = "row"
    file.content += indent(f"TableRow {root_var_name} = new TableRow();", depth)
    parser_handle_fields(repository, depth, root_var_name, field, root_var, file)
    file.content += indent(f"return Collections.singletonList({root_var_name});", depth)


def generate_parser(repository, parserName, field, file):
    """
    Entrypoint for the code generator.

    :param repository: the repository of message types and enums
    :param field: root field for the table
    :param file: output file to write code to
    :return:
    """
    outer_name = "EventOuterClass"  # TODO
    path = to_variable(field['name'])
    file.content += indent("// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!", 0)
    file.content += indent(f"package {field['package']}; \n", 0)
    file.content += indent(f"import com.google.api.services.bigquery.model.TableRow;", 0)
    file.content += indent(f"import com.google.api.services.bigquery.model.TableCell;\n", 0)
    file.content += indent(f"import java.util.LinkedList;", 0)
    file.content += indent(f"import java.util.List;\n", 0)
    file.content += indent(f"public final class {parserName} {{\n", 0)
    file.content += indent(f"public static List<TableRow> convertToTableRow({outer_name}.{field['name']} {path}) throws Exception {{", 1)

    batch_field_found = False
    for f in field["fields"]:
        if f["isBatchField"]:
            batch_field_found = True
            parser_handle_batch_table(repository, outer_name, 2, path, field, f, file)

    if not batch_field_found:
        parser_handle_table(repository, 2, path, field, file)

    file.content += indent("}", 1)
    file.content += indent("}", 0)


def generate_code(request, response):
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
