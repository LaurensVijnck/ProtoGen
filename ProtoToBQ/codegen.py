from abc import ABC

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

    def __init__(self, name: str, type: str):
        super().__init__(name, type)
        self.name = name
        self.type = type
        self.getters = []

    def add(self, var: Variable):
        return f'{self.name}.add({var.get()});'


class CodeGenImp(ABC):
    """
    Interface for the code generation components.
    """
    @staticmethod
    def indent(s, depth):
        return "\t" * depth + f"{s}"

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        ...


class CodeGenNode(CodeGenImp):
    """
    Abstract node in the code generation tree.
    """
    def __init__(self, field: Field):
        self._field = field
        self._children = []

    def add_child(self, node):
        self._children.append(node)


class CodeGenBaseNode(CodeGenNode):
    """
    Code generation for atomic fields.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        root_var.push_getter(self._field)
        print(self.indent(element.set(self._field.field_name, root_var), depth))
        root_var.pop_getter()


class CodeGenConditionalNode(CodeGenNode):
    """
    Conditional code generation for a given field.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        print(self.indent(f"if({root_var.has(self._field)}) {{", depth))

        for child in self._children:
            child.gen_code(file, element, root_var, depth + 1)

        if self._field.field_required:
            print(self.indent("} else {", depth))
            print(self.indent("throw new Exception();", depth + 1))

        print(self.indent("}", depth))


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

        print(f"")
        print(self.indent(f"// {self._field.field_name}", depth))
        print(self.indent(var.initialize(), depth))

        for child in self._children:
            child.gen_code(file, var, root_var, depth)

        print(self.indent(element.set(self._field.field_name, var), depth))


class CodeGenBatchNode(CodeGenNode):
    """
    Code generation for batch fields
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        rows = ListVariable(Variable.to_variable(f"{self._field.field_name}_list"), "ArrayList<TableRow>")
        root = Variable(Variable.to_variable(self._field.field_name), self._field.field_type)
        row = Variable("row", "TableRow")

        print(self.indent(rows.initialize(), depth))
        print(self.indent(f"for({self._field.field_type_value.name} {root.get()}: {root_var.get()}.getEventsList()) {{", depth))

        print(self.indent(row.initialize(), depth + 1))

        for child in self._children:
            child.gen_code(file, element, root, depth + 1)

        print(self.indent(rows.add(row), depth + 1))

        print(self.indent("}", depth))


class CodeGenFunctionNode(CodeGenImp):
    """
    Code generator for the convertToTableRow function.
    """
    def __init__(self, field_type: MessageFieldType, code_gen_nodes: []):
        self.field_type = field_type
        self.code_gen_nodes = code_gen_nodes

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        variable = Variable(Variable.to_variable(self.field_type.name), self.field_type.name)
        row = Variable("row", "TableRow")

        print(self.indent(f"public static List<TableRow> convertToTableRow({self.field_type.name} {variable.get()}) {{", depth))
        # print(self.indent(row.initialize(), depth + 1))

        for code_gen_node in self.code_gen_nodes:
            code_gen_node.gen_code(None, row, variable, depth + 1)

        print(self.indent("}", depth))


class CodeGenClassNode(CodeGenImp):
    """
    Code generator for the Parser class.
    """
    def __init__(self, field_type: MessageFieldType, func_node: CodeGenFunctionNode):
        self.field_type = field_type
        self.func_node = func_node

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        parser_name = "EventParser"
        print("// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!")
        print(f"package {self.field_type.package};")
        print(f"")
        print(f"public final class {parser_name} {{")
        print(f"")
        self.func_node.gen_code(file, element, root_var, depth + 1)
        print("}")


if __name__ == '__main__':
    type = MessageFieldType("lvi", "file1", "Actor")
    batch_type = MessageFieldType("lvi", "file2", "BatchEvent")
    field = Field(1, "actor", "actor of message", "TYPE_MESSAGE", type, False, False, False)
    batch_field = Field(1, "batch_event", "batch of events", "TYPE_GROUP", batch_type, False, False, False)

    atomic1 = Field(2, "name", "", "TYPE_UINT64", None, False, False, False)
    atomic2 = Field(3, "email", "", "TYPE_STRING", None, True, False, False)

    type2 = MessageFieldType("lvi", "file3", "Address")
    nested_type = Field(4, "address", "address of the actor", "TYPE_MESSAGE", type2, False, False, False)
    nested1 = Field(5, "steet", "", "TYPE_STRING", None, False, False, False)
    nested2 = Field(6, "number", "", "TYPE_STRING", None, False, False, False)
    nested3 = Field(7, "city", "", "TYPE_STRING", None, False, False, False)

    batch = CodeGenNestedNode(field)

    root = CodeGenBatchNode(batch_field)
    root.add_child(batch)

    conditional = CodeGenConditionalNode(field)
    c2 = CodeGenConditionalNode(atomic2)
    batch.add_child(conditional)

    get1 = CodeGenGetFieldNode(field)
    conditional.add_child(get1)

    get1.add_child(CodeGenBaseNode(atomic1))
    get1.add_child(c2)
    c2.add_child(CodeGenBaseNode(atomic2))

    nested = CodeGenNestedNode(nested_type)
    c3 = CodeGenConditionalNode(nested_type)
    c3.add_child(CodeGenBaseNode(nested1))
    c3.add_child(CodeGenBaseNode(nested2))
    c3.add_child(CodeGenBaseNode(nested3))
    nested.add_child(c3)
    conditional.add_child(nested)

    event = MessageFieldType("lvi", "file1", "Event")
    func_node = CodeGenFunctionNode(event, [root])
    cl = CodeGenClassNode(event, func_node)
    cl.gen_code(None, None, None, 0)