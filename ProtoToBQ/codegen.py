from abc import ABC

import logging
from fields import *
import re


class Variable:
    """
    Representation of a variable.
    """
    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type
        self.getters = []

    def push_getter(self, field: Field):
        self.getters.append(field)

    def pop_getter(self):
        self.getters.pop()

    def has(self, field: Field):
        return self.get() + self.underscore_to_camelcase(f".has_{field.field_name}()")

    def get(self):
        return self.name + "".join([self.underscore_to_camelcase(f".get_{getter.field_name}()") for getter in self.getters])

    def set(self, attribute: str, variable):
        return f'{self.get()}.set("{attribute}", {variable.get()});'

    def initialize(self):
        return f'{self.type} {self.name} = new {self.type}();'

    @staticmethod
    def underscore_to_camelcase(s):
        return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)

    @staticmethod
    def to_variable(s):
        if len(s) > 0:
            return s[0].lower() + Variable.underscore_to_camelcase(s[1:])
        return s


class ListVariable(Variable):
    """
    Representation of a list variable.
    """
    def __init__(self, name: str, list_type: str, value_type: str):
        super().__init__(name, f'{list_type}<{value_type}>')

    def add(self, var: Variable):
        return f'{self.name}.add({var.get()});'


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

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        ...

    @staticmethod
    def indent(s, depth):
        return "\t" * depth + f"{s}\n"


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
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        root_var.push_getter(self._field)
        file.content += self.indent(element.set(self._field.field_name, root_var), depth)
        root_var.pop_getter()


class CodeGenConditionalNode(CodeGenNode):
    """
    Conditional code generation for a given field.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        file.content += self.indent(f"if({root_var.has(self._field)}) {{", depth)

        for child in self._children:
            child.gen_code(file, element, root_var, depth + 1)

        if self._field.field_required:
            file.content += self.indent("} else {", depth)
            file.content += self.indent("throw new Exception();", depth + 1)

        file.content += self.indent("}", depth)


class CodeNopNode(CodeGenImp):

    def __init__(self, table_root: bool, batch_table: bool):
        super().__init__()
        self._table_root = table_root
        self._batch_table = batch_table
        self._variable = None

    def get_variable(self):
        return self._variable

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):

        new_root = element
        if self._table_root:
            self._variable = Variable("common", "TableRow")
            file.content += self.indent(self._variable.initialize(), depth)
            new_root = self._variable

        for child in self._children:
            child.gen_code(file, new_root, root_var, depth)

        if self._table_root and not self._batch_table:
            file.content += self.indent(element.add(self._variable), depth)


class CodeGenGetBatchNode(CodeGenNode):
    """
    Code generation for batched nodes.
    """
    def __init__(self, field: Field, neighbour: CodeNopNode):
        super().__init__(field)
        self._neighbour = neighbour

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        # Batch attribute
        root = Variable(Variable.to_variable(self._field.field_type_value.name), self._field.field_type_value.name)
        row = Variable("row", "TableRow")

        file.content += self.indent(f"for({self._field.field_type_value.name} {root.get()}: {root.get()}: {root_var.get()}.{Variable.underscore_to_camelcase(f'get_{self._field.field_name}_list()')}) {{", depth) # nopep8
        file.content += self.indent(row.initialize(), depth + 1)

        for child in self._children:
            child.gen_code(file, row, root, depth + 1)

        # FUTURE: Could check if neighbour actually has children
        variable = self._neighbour.get_variable()
        if variable is not None:
            file.content += self.indent(f"{row.get()}.setF({variable.get()}.getF());", depth + 1)  # TODO Move to variable class?

        file.content += self.indent(element.add(row), depth + 1)
        file.content += self.indent("}", depth)


class CodeGenGetFieldNode(CodeGenNode):
    """
    Code generation to apply a getter on the root variable.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        root_var.push_getter(self._field)

        for child in self._children:
            child.gen_code(file, element, root_var, depth)

        root_var.pop_getter()


class CodeGenNestedNode(CodeGenNode):
    """
    Code generation for (nested) message fields
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        var = Variable(Variable.to_variable(self._field.field_name), "TableCell")

        # file.content += self.indent(f"// {self._field.field_name}", depth)
        file.content += self.indent(var.initialize(), depth)

        for child in self._children:
            child.gen_code(file, var, root_var, depth)

        file.content += self.indent(element.set(self._field.field_name, var), depth)


class CodeGenFunctionNode(CodeGenImp):
    """
    Code generator for the convertToTableRow function.
    """
    def __init__(self, field_type: MessageFieldType):
        super().__init__()
        self.field_type = field_type

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        variable = Variable(Variable.to_variable(self.field_type.name), self.field_type.name)
        rows = ListVariable("rows", "LinkedList", "TableRow")

        file.content += self.indent(f"public static LinkedList<TableRow> convertToTableRow({self.field_type.name} {variable.get()}) throws Exception {{", depth)
        file.content += self.indent(rows.initialize(), depth + 1)

        for child in self._children:
            child.gen_code(file, rows, variable, depth + 1)

        file.content += self.indent(f"return {rows.get()};", depth + 1)
        file.content += self.indent("}", depth)


class CodeGenClassNode(CodeGenImp):
    """
    Code generator for the Parser class.
    """
    def __init__(self, field_type: MessageFieldType):
        super().__init__()
        self.field_type = field_type

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        parser_name = "EventParser"
        file.content += self.indent("// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!", depth)
        file.content += self.indent(f"package {self.field_type.package};", depth)
        file.content += self.indent("", depth)
        file.content += self.indent(f"import com.google.api.services.bigquery.model.TableRow;", depth)
        file.content += self.indent(f"import com.google.api.services.bigquery.model.TableCell;", depth)
        file.content += self.indent("", depth)
        file.content += self.indent(f"import java.util.LinkedList;", depth)
        file.content += self.indent("", depth)
        file.content += self.indent(f"public final class {parser_name} {{", depth)

        for child in self._children:
            child.gen_code(file, element, root_var, depth + 1)

        file.content += self.indent("}", depth)