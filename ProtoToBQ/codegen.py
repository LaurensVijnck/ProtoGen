from fields import *
import re


class Variable:
    """
    Representation of a variable.
    """
    def __init__(self, name: str):
        self.name = name
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
        return f'{self.get()}.set("{attribute}", {variable.get()})'

    @staticmethod
    def underscore_to_camelcase(s):
        return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)

    @staticmethod
    def to_variable(s):
        if len(s) > 0:
            return s[0].lower() + Variable.underscore_to_camelcase(s[1:])
        return s


class CodeGenNode:
    """
    Abstract node in the code generation tree.
    """
    def __init__(self, field: Field):
        self._field = field
        self._children = []

    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        ...

    def add_child(self, node):
        self._children.append(node)

    @staticmethod
    def indent(s, depth):
        return "\t" * depth + f"{s}"


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
    Code generation for conditional.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        print(self.indent(f"if({root_var.has(self._field)}) {{", depth))
        for child in self._children:
            child.gen_code(file, element, root_var, depth + 1)
        print(self.indent("}", depth))


class CodeGenGetFieldNode(CodeGenNode):
    """
    Code generation for conditional.
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        root_var.push_getter(self._field)
        for child in self._children:
            child.gen_code(file, element, root_var, depth)

        root_var.pop_getter()


class CodeGenNestedNode(CodeGenNode):
    """
    Code generation for message fields
    """
    def gen_code(self, file, element: Variable, root_var: Variable, depth: int):
        var = Variable(Variable.to_variable(self._field.field_name))
        print(self.indent(f"TableCell {var.get()} = new TableCell();", depth))
        for child in self._children:
            child.gen_code(file, var, root_var, depth)

        print(self.indent(element.set(self._field.field_name, var), depth))


if __name__ == '__main__':
    type = MessageFieldType("lvi", "file1", "actor")
    field = Field(1, "actor", "actor of message", "TYPE_MESSAGE", type, False, False, False)
    atomic1 = Field(2, "name", "", "TYPE_UINT64", None, False, False, False)
    atomic2 = Field(2, "email", "", "TYPE_STRING", None, False, False, False)

    root = CodeGenNestedNode(field)

    conditional = CodeGenConditionalNode(field)
    c2 = CodeGenConditionalNode(atomic2)
    root.add_child(conditional)

    get1 = CodeGenGetFieldNode(field)
    conditional.add_child(get1)

    get1.add_child(CodeGenBaseNode(atomic1))
    get1.add_child(c2)
    c2.add_child(CodeGenBaseNode(atomic2))

    root.gen_code(None, Variable("row"), Variable("event"), 0)