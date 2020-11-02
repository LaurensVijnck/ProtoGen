import json


class FieldType:
    """
    Abstract representation of a field type.
    """
    def __init__(self, package, file_name, name):
        self.package = package
        self.file_name = file_name
        self.name = name

    def get_fq_name(self):
        return f".{self.package}.{self.name}"

    def to_json(self):
        ...

    @staticmethod
    def to_fq_name(package, name):
        return f".{package}.{name}"


class MessageFieldType(FieldType):
    """
    Representation of a message field type, i.e., a field containing one or more fields.
    """
    def __init__(self, package, file_name, name):
        FieldType.__init__(self, package, file_name, name)
        self.fields = None
        self.table_root = False

    def set_fields(self, fields: list):
        self.fields = fields

    def set_table_root(self, table_root: bool):
        self.table_root = table_root

    def to_json(self):
        return {
            "package": self.package,
            "filename": self.file_name,
            "name": self.name,
            "table_root": self.table_root,
            "fields": [f.to_json() for f in self.fields]
        }


class EnumFieldType(FieldType):
    """
    Representation of an enum field type, i.e., a field with a mapping from names to values.
    """
    def __init__(self, package, file_name, name):
        FieldType.__init__(self, package, file_name, name)
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
    Representation of a field.
    """
    def __init__(self, field_index: int, field_name: str, field_description: str, field_type: str, field_type_value: MessageFieldType, field_required: bool, is_batch_field: bool, is_optional_field: bool):
        self.field_index = field_index
        self.field_name = field_name
        self.field_description = field_description
        self.field_type = field_type
        self.field_type_value = field_type_value
        self.field_required = field_required
        self.is_batch_field = is_batch_field
        self.is_optional_field = is_optional_field

    def to_json(self):
        return {
            "index": self.field_index,
            "name": self.field_name,
            "description": self.field_description,
            "type": self.field_type,
            "type_value": self.field_type_value.get_fq_name() if self.field_type_value is not None else None,
            "required": self.field_required,
            "batch_field": self.is_batch_field,
            "optional_field": self.is_optional_field
        }