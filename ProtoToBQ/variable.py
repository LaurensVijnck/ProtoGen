from abc import ABC
from fields import *
import re


class Value(ABC):
    def __init__(self, type: str):
        self.type = type

    def format_value(self, syntax) -> str:
        """
        Function to format the value in the specified programming language.

        :param syntax:
        :return:
        """
        ...


class StaticValue(Value):
    def __init__(self, value):
        super().__init__("") # TODO Infer the type from the value
        self.value = value

    def format_value(self, syntax) -> str:
        return syntax.format_constant_value(self.value)


# FUTURE: Distinction between variable and variables with constant values
class Variable(Value):
    """
    Representation of a variable.
    """
    def __init__(self, name: str, type: str):
        super().__init__(type)
        self.name = name
        self.getters = []

    def push_getter(self, field: Field):
        self.getters.append(field)

    def pop_getter(self):
        self.getters.pop()

    def format_value(self, syntax) -> str:
        return syntax.unroll_getters(self)

    # TODO: Move to JavaSyntax
    def has(self, field: Field):
        return self.get() + self.underscore_to_camelcase(f".has_{field.field_name}()")

    # TODO: Move to JavaSyntax
    def get(self):
        return self.name + "".join([self.underscore_to_camelcase(f".get_{getter.field_name}()") for getter in self.getters])

    # TODO: Move to JavaSyntax
    def set(self, attribute: str, value):
        return f'{self.get()}.set("{attribute}", {value});'

    # TODO: Move to JavaSyntax
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

    @staticmethod
    def to_upper_camelcase(s):
        if len(s) > 0:
            return s[0].upper() + Variable.underscore_to_camelcase(s[1:])
        return s

    # TODO Move to JavaSyntax
    @staticmethod
    def format_constant_value(val):

        if val is None:
            return "null"

        if isinstance(val, str):
            return f'"{val}"'

        if isinstance(val, float):
            return f'{val}f'

        return val


class ListVariable(Variable):
    """
    Representation of a list variable.
    """
    def __init__(self, name: str, list_type: str, value_type: str):
        super().__init__(name, f'{list_type}<{value_type}>')

    def add(self, var: Variable):
        return f'{self.name}.add({var.get()});'