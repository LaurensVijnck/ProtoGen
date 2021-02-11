from enum import Enum


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