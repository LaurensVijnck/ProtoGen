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

    def set(self, attribute: str, value):
        return f'{self.get()}.set("{attribute}", {value});'

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