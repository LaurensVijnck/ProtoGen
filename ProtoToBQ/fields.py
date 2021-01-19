import json

from enums import ProtoTypeEnum


class FieldType:
    """
    Abstract representation of a field type.
    """
    def __init__(self, package, file_name, name, ambiguous_file_name):
        self.package = package
        self.file_name = file_name
        self.name = name
        self.ambiguous_file_name = ambiguous_file_name

    def get_fq_name(self):
        return f".{self.package}.{self.name}"

    def get_class_name(self):
        # FUTURE: This should be extracted in the code-gen class, as it's Java specific.
        if not self.ambiguous_file_name:
            return f"{self.file_name[0].upper() + self.file_name[1:]}.{self.name}"

        return f"{self.file_name[0].upper() + self.file_name[1:]}OuterClass.{self.name}"

    def to_json(self):
        ...

    @staticmethod
    def to_fq_name(package, name):
        return f".{package}.{name}"


class MessageFieldType(FieldType):
    """
    Representation of a message field type, i.e., a proto message containing one or more fields.
    """
    def __init__(self, package, file_name, name, ambiguous_file_name):
        FieldType.__init__(self, package, file_name, name, ambiguous_file_name)
        self.fields = None
        self.table_root = False
        self.batch_table = False
        self.table_name =  None
        self.cluster_fields = []
        self.partition_field = None
        self.table_description = None

    def set_fields(self, fields: list):
        self.fields = fields

    def set_table_root(self, table_root: bool, table_name: str, table_description: str):
        self.table_root = table_root
        self.table_name = table_name
        self.table_description = table_description

    def set_batch_table(self, batch_table: bool):
        self.batch_table = batch_table

    def add_cluster_field(self, cluster_field: str):
        self.cluster_fields.append(cluster_field)

    def set_partition_field(self, partition_field: str):
        self.partition_field = partition_field

    def to_json(self):
        return {
            "package": self.package,
            "filename": self.file_name,
            "ambiguous_file_name": self.ambiguous_file_name,
            "name": self.name,
            "table_root": self.table_root,
            "batch_table": self.batch_table,
            "cluster_fields": self.cluster_fields,
            "partition_field": self.partition_field,
            "table_name": self.table_name,
            "table_description": self.table_description,
            "fields": [f.to_json() for f in self.fields]
        }


class EnumFieldType(FieldType):
    """
    Representation of an enum field type, i.e., a field with a mapping from names to values.
    """
    def __init__(self, package, file_name, name, ambiguous_file_name):
        FieldType.__init__(self, package, file_name, name, ambiguous_file_name)
        self.values = None

    def set_values(self, values: list):
        self.values = values

    def to_json(self):
        return {
            "package": self.package,
            "filename": self.file_name,
            "name": self.name,
            "fields": json.dumps({v.name: v.value for v in self.values})
        }


class EnumFieldValue:
    """
    Representation for a value in the enum field, i.e., a mapping from name to value.
    """
    def __init__(self, name, value):
        self.name = name
        self.value = value


class Field:
    """
    Representation of a field, i.e., an entry in a proto message.
    """
    def __init__(self, field_index: int,
                 field_name: str,
                 field_description: str,
                 field_type: str,
                 field_type_value: MessageFieldType,
                 field_required: bool,
                 is_batch_field: bool,
                 is_partitioning_field: bool,
                 is_clustering_field: bool,
                 is_optional_field: bool,
                 is_repeated_field: bool,
                 default_value = None):

        self.field_index = field_index
        self.field_name = field_name
        self.field_description = field_description
        self.field_type = field_type
        self.field_type_value = field_type_value
        self.field_required = field_required
        self.is_batch_field = is_batch_field
        self.is_partitioning_field = is_partitioning_field
        self.is_clustering_field = is_clustering_field
        self.is_optional_field = is_optional_field
        self.is_repeated_field = is_repeated_field
        self.default_value = default_value

    def resolve_type(self, type_map: dict):
        """
        Resolve the type of the field against the given type dictionary.

        :param type_map: mapping from proto enum to primitive types
        :return:
        """

        if self.field_type_value is not None:
            return self.field_type_value.get_class_name()

        # Primitive types are determined by the type_map
        return type_map[ProtoTypeEnum._member_map_[self.field_type]]

    def to_json(self):
        return {
            "index": self.field_index,
            "name": self.field_name,
            "description": self.field_description,
            "type": self.field_type.name,
            "type_value": self.field_type_value.get_fq_name() if self.field_type_value is not None else None,
            "required": self.field_required,
            "batch_field": self.is_batch_field,
            "clustering_field": self.is_clustering_field,
            "partitioning_field": self.is_partitioning_field,
            "optional_field": self.is_optional_field,
            "repeated_field": self.is_repeated_field,
            "default_value": self.default_value
        }