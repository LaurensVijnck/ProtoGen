from abc import ABC

import logging
from variable import *


class CodeGenImp(ABC):
    """
    Interface for the code generation components.
    """
    def __init__(self):
        self._children = []

    def add_child(self, node):
        self._children.insert(0, node)

    def add_children(self, children):
        self._children.extend(children)

    def get_num_children(self):
        return len(self._children)

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        ...

    @staticmethod
    def indent(s, depth):
        return "\t" * depth + f"{s}\n"


class CodeGenInterfaceNode(CodeGenImp):
    """
    Code generator for the parser interface

    TODO Interface is quite hardcoded, requires a refinement step.
    """
    def __init__(self, package_name: str, class_name: str, function_name: str, parsers: dict):
        super().__init__()
        self.package_name = package_name
        self.class_name = class_name
        self.function_name = function_name
        self.parsers = parsers

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        file.content += self.indent("// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!", depth)
        file.content += self.indent(f"package {self.package_name};", depth)

        # BigQuery imports
        file.content += self.indent("", depth)
        file.content += self.indent(f"import com.google.api.services.bigquery.model.*;", depth)

        # Java imports
        file.content += self.indent("", depth)
        file.content += self.indent(f"import java.util.LinkedList;", depth)
        file.content += self.indent("", depth)

        obj = Variable("obj", "byte[]")
        file.content += self.indent(f"public abstract class {Variable.to_upper_camelcase(self.class_name)} {{", depth)

        # Generate abstract function
        file.content += self.indent("", depth)
        file.content += self.indent(f"public abstract LinkedList<TableRow> {Variable.to_variable(self.function_name)}({obj.type} {obj.get()}) throws Exception;", depth + 1)

        # Generate table name extractor function
        file.content += self.indent("", depth)
        file.content += self.indent('public abstract String getBigQueryTableName();', depth + 1)

        # Generate table description extractor function
        file.content += self.indent("", depth)
        file.content += self.indent('public abstract String getBigQueryTableDescription();', depth + 1)

        # Generate partition field extractor function
        file.content += self.indent("", depth)
        file.content += self.indent('public abstract TimePartitioning getPartitioning();', depth + 1)

        # Generate cluster fields extractor function
        file.content += self.indent("", depth)
        file.content += self.indent(f'public abstract Clustering getClustering();', depth + 1)

        # Schema extractor function
        file.content += self.indent("", depth)
        file.content += self.indent('public abstract TableSchema getBigQueryTableSchema();', depth + 1)

        # Generate repository function
        # FUTURE: By far not the most elegant approach, but I required a way to fetch parsers given their name.
        file.content += self.indent("", depth)
        proto_type = Variable(Variable.to_variable("proto_type"), "String")
        file.content += self.indent(f"public static {Variable.to_upper_camelcase(self.class_name)} getParserForType({proto_type.type} {proto_type.get()}) throws Exception {{", depth + 1)
        file.content += self.indent(f"switch({proto_type.get()}) {{", depth + 2)

        for type, parser in self.parsers.items():
            file.content += self.indent(f'case "{type}":', depth + 3)
            file.content += self.indent(f'return new {parser}();', depth + 4)

        file.content += self.indent(f'default:', depth + 3)
        file.content += self.indent(f'throw new Exception("Parser for type \'" + {proto_type.get()} + "\' not registered.");',depth + 4)
        file.content += self.indent("}", depth + 2)
        file.content += self.indent("}", depth + 1)

        file.content += self.indent("}", depth)


class CodeGenClassNode(CodeGenImp):
    """
    Code generator for the Parser class.
    """
    def __init__(self, class_name: str, base_class: str, field_type: MessageFieldType):
        super().__init__()
        self.class_name = class_name
        self.field_type = field_type
        self.base_class = base_class

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        file.content += self.indent("// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!", depth)
        file.content += self.indent(f"package {self.field_type.package};", depth)

        # BigQuery imports
        file.content += self.indent("", depth)
        file.content += self.indent(f"import com.google.api.services.bigquery.model.*;", depth)

        # Java imports
        file.content += self.indent("", depth)
        file.content += self.indent(f"import java.util.LinkedList;", depth)
        file.content += self.indent(f"import java.util.List;", depth)
        file.content += self.indent(f"import java.util.Arrays;", depth)
        file.content += self.indent(f"import org.joda.time.Instant;", depth) # Requires a custom Maven dependency
        file.content += self.indent("", depth)

        file.content += self.indent(f"public final class {Variable.to_upper_camelcase(self.class_name)} extends {self.base_class} {{", depth)

        for child in self._children:
            child.gen_code(file, element, root_var, depth + 1, type_map)

        # Generate table name extractor function
        file.content += self.indent("", depth)
        file.content += self.indent(f"public String getBigQueryTableName() {{", depth + 1)
        file.content += self.indent(f'return {Variable.format_constant_value(self.field_type.table_name)};', depth + 2)
        file.content += self.indent("}", depth + 1)

        # Generate table description extractor function
        file.content += self.indent("", depth)
        file.content += self.indent(f"public String getBigQueryTableDescription() {{", depth + 1)
        file.content += self.indent(f'return {Variable.format_constant_value(self.field_type.table_description)};', depth + 2)
        file.content += self.indent("}", depth + 1)

        # Generate partition field extractor function
        file.content += self.indent("", depth)
        file.content += self.indent(f"public TimePartitioning getPartitioning() {{", depth + 1)

        if self.field_type.partition_field is None:
            file.content += self.indent(f'return {Variable.format_constant_value(None)};', depth + 2)
        else:
            file.content += self.indent(f'return new TimePartitioning().setField({Variable.format_constant_value(self.field_type.partition_field)});', depth + 2)
        file.content += self.indent("}", depth + 1)

        # Generate cluster fields extractor function
        file.content += self.indent("", depth)
        file.content += self.indent(f"public Clustering getClustering() {{", depth + 1)

        if len(self.field_type.cluster_fields) == 0:
            file.content += self.indent(f'return {Variable.format_constant_value(None)};', depth + 2)
        else:
            file.content += self.indent(f'return new Clustering()',depth + 2)
            file.content += self.indent(f'.setFields(Arrays.asList({", ".join([Variable.format_constant_value(field) for field in self.field_type.cluster_fields])}));',depth + 3)
        file.content += self.indent("}", depth + 1)

        file.content += self.indent("}", depth)


class CodeGenSchemaFunctionNode(CodeGenImp):
    """
    Code generation for schema extraction

    FUTURE: Node currently does not adhere the code generation node approach. Might need conversion in the future.
    """
    def __init__(self, field_type: MessageFieldType, bq_type_map: dict):
        super().__init__()
        self.field_type = field_type
        self.bq_type_map = bq_type_map

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):

        file.content += self.indent(f"public TableSchema getBigQueryTableSchema() {{", depth)

        # Generate return
        file.content += self.indent(f'return new TableSchema().setFields(Arrays.asList(', depth + 1)
        self._codegen_schema(file, self.field_type, depth + 2)
        file.content += self.indent('));', depth + 1)

        file.content += self.indent("}", depth)

    def _codegen_schema_field(self, file, field: Field, depth: int):
        if field.is_batch_field:
            # Batch fields should be skipped, by immediately fetching the next level
            return self._codegen_schema(file, field.field_type_value, depth)

        file.content += self.indent(f'new TableFieldSchema()', depth)
        file.content += self.indent(f'.setName({Variable.format_constant_value(field.field_name)})', depth + 1)

        # FUTURE: Enhance logic to represent timestamps
        file.content += self.indent(f'.setType({Variable.format_constant_value(self.bq_type_map[field.field_type] if not field.is_timestamp else "TIMESTAMP")})', depth + 1)
        file.content += self.indent(f'.setMode("{"REPEATED" if field.is_repeated_field else "REQUIRED" if field.field_required else "NULLABLE"}")', depth + 1)
        file.content += self.indent(f'.setDescription("{field.field_description}")', depth + 1)

        if field.field_type_value is not None:
            file.content += self.indent(f'.setFields(Arrays.asList(', depth + 1)
            self._codegen_schema(file, field.field_type_value, depth + 2)
            file.content += self.indent(f'))', depth + 1)

    def _codegen_schema(self, file, field_type: MessageFieldType, depth: int):

        for idx, field in enumerate(field_type.fields):
            self._codegen_schema_field(file, field, depth)

            if idx < len(field_type.fields) - 1:
                file.content += self.indent(f',', depth)


class CodeGenFunctionNode(CodeGenImp):
    """
    Code generator for the convertToTableRow function.
    """
    def __init__(self, function_name: str, field_type: MessageFieldType):
        super().__init__()
        self.function_name = function_name
        self.field_type = field_type

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        obj = Variable("obj", "byte[]")
        variable = Variable(Variable.to_variable(self.field_type.name), self.field_type.name)
        rows = ListVariable("rows", "LinkedList", "TableRow")
        file.content += self.indent(f"public {rows.type} {Variable.to_variable(self.function_name)}({obj.type} {obj.get()}) throws Exception {{", depth)

        # Future: extend capabilities of getter function to accept parameters
        file.content += self.indent(f"// Parse bytes according to Protobuf def", depth + 1)
        file.content += self.indent(f"{self.field_type.get_class_name()} {variable.get()} = {self.field_type.get_class_name()}.parseFrom({obj.get()});", depth + 1)

        file.content += self.indent("", depth + 1)
        file.content += self.indent(f"// Initialize result variable", depth + 1)
        file.content += self.indent(rows.initialize(), depth + 1)

        for child in self._children:
            child.gen_code(file, rows, variable, depth + 1, type_map)

        file.content += self.indent(f"return {rows.get()};", depth + 1)
        file.content += self.indent("}", depth)


class CodeNopNode(CodeGenImp):
    """
    Code generator node responsible for creating logical groups.
    """
    def __init__(self, table_root: bool, batch_table: bool):
        super().__init__()
        self._table_root = table_root
        self._batch_table = batch_table
        self._variable = None

    def get_variable(self):
        return self._variable

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):

        new_root = element
        if self._table_root and self.get_num_children() > 0:
            self._variable = Variable("common", "TableRow")
            file.content += self.indent(self._variable.initialize(), depth)
            new_root = self._variable

        for child in self._children:
            child.gen_code(file, new_root, root_var, depth, type_map)

        if self._table_root and not self._batch_table:
            file.content += self.indent(element.add(self._variable), depth)


class CodeGenNode(CodeGenImp):
    """
    Abstract node in the code generation tree.
    """
    def __init__(self, field: Field):
        super().__init__()
        self._field = field


class CodeGenBaseNode(CodeGenNode):
    """
    Code generation for atomic fields.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        # FUTURE: Enhance logic to represent timestamps
        # This can be done by using a 'general' mapper function
        # for each field, prior to extracting them.
        file.content += self.indent(element.set(self._field.get_field_name(), root_var.get() if not self._field.is_timestamp else f"Instant.ofEpochMilli({root_var.get()}).toString()"), depth)


class CodeGenConditionalNode(CodeGenNode):
    """
    Conditional code generation for a given field, i.e., node checks whether field value is available.
    """
    def __init__(self, field: Field, throw_exception = False, default_value=None):
        super().__init__(field)
        self._throw_exception = throw_exception
        self._default_value = default_value

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        file.content += self.indent(f"if({root_var.has(self._field)}) {{", depth)

        for child in self._children:
             child.gen_code(file, element, root_var, depth + 1, type_map)

        if self._default_value is not None or self._throw_exception:

            file.content += self.indent("} else {", depth)

            # Fall back onto the fault value
            if self._default_value is not None:
                file.content += self.indent(element.set(self._field.get_field_name(), Variable.format_constant_value(self._default_value)), depth + 1)

            # Otherwise throw exception
            else:
                field_path = [x.field_name for x in root_var.getters] + [self._field.get_field_name()]
                file.content += self.indent(f'throw new Exception("Required attribute \'{".".join(field_path)}\' not found on input.");', depth + 1)

        file.content += self.indent("}", depth)


class CodeGenGetFieldNode(CodeGenNode):
    """
    Code generation to apply a getter on the root variable.
    """
    def __init__(self, field: Field):
        super().__init__(field)

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        root_var.push_getter(self._field)

        for child in self._children:
            child.gen_code(file, element, root_var, depth, type_map)

        root_var.pop_getter()


class CodeGenNestedNode(CodeGenNode):
    """
    Code generation for (nested) message fields
    """
    def __init__(self, field: Field):
        super().__init__(field)
        self._declaration_required = not self._field.is_repeated_field

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):

        var = Variable(Variable.to_variable(self._field.field_name), "TableCell")

        file.content += self.indent("", depth)
        file.content += self.indent(f"// {self._field.field_name}", depth)
        file.content += self.indent(var.initialize(), depth)

        for child in self._children:
            child.gen_code(file, var, root_var, depth, type_map)

        file.content += self.indent(element.set(self._field.get_field_name(), var.get()), depth)


class CodeGenRepeatedNode(CodeGenNode):
    """
    Code generation for repeated atomic fields.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        list_var = ListVariable(Variable.to_variable(f"{self._field.field_name}_cells"), "LinkedList", "TableCell")
        var = Variable(Variable.to_variable(f"{self._field.field_name}_cell"), "TableCell")
        res = Variable(Variable.to_variable(self._field.field_name), self._field.resolve_type(type_map))

        file.content += self.indent(list_var.initialize(), depth)

        file.content += self.indent(f"for({res.type} {res.get()}: {root_var.get()}.{Variable.underscore_to_camelcase(f'get_{self._field.field_name}_list()')}) {{", depth)  # nopep8
        file.content += self.indent(var.initialize(), depth + 1)

        for child in self._children:
            child.gen_code(file, var, res, depth + 1, type_map)

        file.content += self.indent(list_var.add(var), depth + 1)
        file.content += self.indent("}", depth)
        file.content += self.indent(element.set(self._field.get_field_name(), list_var.get()), depth)


class CodeGenGetBatchNode(CodeGenNode):
    """
    Code generation for batched nodes, batched nodes are build up of a common and non-common
    section. The common section is applied to each non-common entry.
    """
    def __init__(self, field: Field, neighbour: CodeNopNode):
        super().__init__(field)
        self._neighbour = neighbour

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int, type_map: dict):
        root = Variable(Variable.to_variable(self._field.field_name), self._field.resolve_type(type_map))
        row = Variable("row", "TableRow")

        file.content += self.indent(f"for({root.type} {root.get()}: {root_var.get()}.{Variable.underscore_to_camelcase(f'get_{self._field.field_name}_list()')}) {{", depth) # nopep8
        file.content += self.indent(row.initialize(), depth + 1)

        for child in self._children:
            child.gen_code(file, row, root, depth + 1, type_map)

        variable = self._neighbour.get_variable()
        if variable is not None:
            # Merge contents of the batch row with the contents
            # of the shared row, if it exists.
            file.content += self.indent(f"for (String key: {variable.get()}.keySet()) {{", depth + 1)
            file.content += self.indent(f"{row.get()}.set(key, {variable.get()}.get(key));", depth + 2)
            file.content += self.indent("}", depth + 1)

        file.content += self.indent(element.add(row), depth + 1)
        file.content += self.indent("}", depth)




