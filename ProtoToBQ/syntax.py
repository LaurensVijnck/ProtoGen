from abc import ABC
from variable import *


class LanguageSyntax(ABC):
    """
    Abstract class to define the syntax for a programming language.
    """

    def unroll_getters(self, variable: Variable) -> str:
        """
        Function to unroll the getters applied on the variable.

        :param variable: variable to unroll getters
        :return:
        """

    def generate_function_invocation(self, variable: Variable, function_name: str, params: [Variable]) -> str:
        """
        Function to invoke a function on the specified variable.

        :param variable: variable to invoke function of.
        :param function_name: function name to invoke.
        :param params: params to invoke the function with.
        :return:
        """
        ...

    def to_variable_name(self, name: str) -> str:
        """
        Function to format the given name as a valid variable name.

        :param name: name of the variable
        :return: name formatted according to the language spec.
        """
        ...

    def to_function_name(self, name: str) -> str:
        """
        Function to format the given name as a valid function name.

        :param name: name of the variable
        :return: name formatted according to the language spec.
        """
        ...

    def to_class_name(self, name: str) -> str:
        """
        Function to format the given name as a valid class name.

        :param name: name of the class
        :return: name formatted according to the language spec.
        """

    def format_constant_value(self, val: object) -> str:
        """
        Function to format the given value as a constant value in the programming language

        :param val: value to format
        :return: value formatted according to the language spec.
        """
        ...


class JavaSyntax(LanguageSyntax):
    """
    Class that defines the syntax of the Java programming language.
    """

    def unroll_getters(self, variable: Variable) -> str:
        # TODO Unsure if this is the right location to do this, maybe move to variable
        return variable.name + "".join([self.underscore_to_camelcase(f".get_{getter.field_name}()") for getter in variable.getters])

    def generate_function_invocation(self, variable: Variable, function_name: str, params: [str]) -> str:
        return f"{self.unroll_getters(variable)}.{function_name}({', '.join(params)})"

    def to_function_name(self, name: str) -> str:
        return self.to_variable_name(name)

    def to_variable_name(self, name: str) -> str:
        # Implements lowerCamelCase
        if len(name) > 0:
            return name[0].lower() + self.underscore_to_camelcase(name[1:])
        return name

    def to_class_name(self, name: str) -> str:
        # Implements upperCamelCase
        if len(name) > 0:
            return self.underscore_to_camelcase(name)
        return name

    def format_constant_value(self, val: object) -> str:
        # Limitation: Unable to determine long type in Pyhon
        if val is None:
            return "null"

        if isinstance(val, str):
            return f'"{val}"'

        if isinstance(val, float):
            return f'{val}f'

        return val

    @staticmethod
    def underscore_to_camelcase(s: str) -> str:
        return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)
