#!/usr/bin/env python
from enum import Enum
import itertools
import json
import sys
import logging

from output.python.protos import bigquery_options_pb2
from google.protobuf.compiler import plugin_pb2 as plugin
from google.protobuf.descriptor_pb2 import DescriptorProto, EnumDescriptorProto


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
  ProtoTypeEnum.TYPE_ENUM: BigQueryTypeEnum.TYPE_STRING, # FUTURE: Map onto int value
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
  ProtoTypeEnum.TYPE_GROUP:  BigQueryTypeEnum.TYPE_STRUCT
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


def _generate_repository(request):
    """
    Collects all known message and enum types.
    :param request:
    :return: the root elements and a repository with all known message and enum types
    """

    repository = {}
    root_el = []

    for proto_file in request.proto_file:

        # Parse request
        for item, package in _traverse(proto_file):

            if isinstance(item, DescriptorProto) or isinstance(item, EnumDescriptorProto):
                data = {
                    'package': proto_file.package or '&lt;root&gt;',
                    'filename': proto_file.name,
                    'name': item.name,
                }

                if isinstance(item, DescriptorProto):

                    if item.options.HasExtension(bigquery_options_pb2.table_root):
                        root_el.append(f".{proto_file.package}.{item.name}")

                    # https://googleapis.dev/python/protobuf/latest/google/protobuf/descriptor.html
                    # logging.warning(item.DESCRIPTOR.fields_by_name.keys())
                    # logging.warning(item.DESCRIPTOR.extensions_by_name.keys())
                    # logging.warning(item.DESCRIPTOR.options)
                    for f in item.field:
                        # https://googleapis.dev/python/protobuf/latest/google/protobuf/descriptor_pb2.html#module-google.protobuf.descriptor_pb2
                        has_option_specified = f.options.HasExtension(bigquery_options_pb2.required)
                        is_required = f.options.Extensions[bigquery_options_pb2.required] # Defaults to False
                        logging.warning("Field: %s option present: %s required: %s", f.name, has_option_specified, is_required)

                    data.update({
                        # https://stackoverflow.com/questions/32836315/python-protocol-buffer-field-options/32867712#32867712
                        "root": item.options.HasExtension(bigquery_options_pb2.table_root),
                        'type': 'Message',
                        'fields': [
                            {
                                'fieldName': f.name,
                                'fieldDescription': f.options.Extensions[bigquery_options_pb2.description],
                                'fieldType': ProtoTypeEnum(f.type).name,
                                'fieldTypeValue': f.type_name,
                                'fieldRequired': f.options.Extensions[bigquery_options_pb2.required],
                                'fieldIndex': f.number,
                                'isBatchField': item.options.Extensions[bigquery_options_pb2.batch_field] == f.name
                             } for f in item.field
                        ]
                    })

                elif isinstance(item, EnumDescriptorProto):
                    data.update({
                        'type': 'Enum',
                        'values': [{'name': v.name, 'value': v.number}
                                   for v in item.value]
                    })

                repository[f".{proto_file.package}.{item.name}"] = data

    return (root_el, repository)


def _contruct_schema_rec(repository, field, schema_arr):
    """
    Constructs a schema for a specific message, given a repository of message types and enums.

    :param repository: the repository of message types and enums
    :param field: the message type for which to create a schema
    :param schema_arr:
    :return: the resulting BQ schema for the message type
    """

    for f in field["fields"]:

        batchField = f["isBatchField"]

        if batchField:
            # Batch fields should be handled seperately, the idea is that the intermediate level is ignored
            schema_arr.extend(_contruct_schema_rec(repository, repository.get(f["fieldTypeValue"]), []))
        else:
            protoType = ProtoTypeEnum._member_map_[f["fieldType"]]
            logging.warning("test: %s %s", f["fieldName"], f['fieldRequired'])

            tableField = {
                "description": f["fieldDescription"],
                "mode": "REQUIRED" if f['fieldRequired'] else "NULLABLE", # FUTURE: Add option to indicate required?
                "name": f["fieldName"],
                "type": _BQ_TO_TYPE_VALUE[_PROTO_TO_BQ_TYPE_MAP[protoType]]
            }

            # Handle complex types
            if protoType == ProtoTypeEnum.TYPE_MESSAGE:
                tableField.update({
                    "fields": _contruct_schema_rec(repository, repository.get(f["fieldTypeValue"]), [])
                })

            schema_arr.append(tableField)

    return schema_arr


def generate_code(request, response):
    """
    :param request: the plugin's input source
    :param response: the plugin's output sink
    :return: None
    """

    # Construct repository
    table_root_el, repository = _generate_repository(request)

    # Generate schema for every root el
    for table_root in table_root_el:
        schema = []
        root = repository[table_root]
        _contruct_schema_rec(repository, root, schema)  # TODO why pass in schema?

        # Fill response
        f = response.file.add()
        f.name = root["name"] + ".json"
        f.content = json.dumps(schema, indent=2)

    # Drop repository
    f = response.file.add()
    f.name = 'repository.json'
    f.content = json.dumps(repository, indent=2)


if __name__ == '__main__':
    # Read request message from stdin
    data = sys.stdin.buffer.read()

    # Parse request
    request = plugin.CodeGeneratorRequest()
    request.ParseFromString(data)

    # Create response
    response = plugin.CodeGeneratorResponse()

    # Generate code
    generate_code(request, response)

    # Serialise response message
    output = response.SerializeToString()

    # Write to stdout
    sys.stdout.buffer.write(output)
