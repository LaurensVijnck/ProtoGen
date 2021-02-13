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

    def generate_function_header(self, name: str, return_type: str, params: [Variable], exceptions: [str] = None, abstract: bool = False) -> str:
        """
        Function to declare a function.

        :param name: name of the function
        :param return_type: return type of the function
        :param params: parameters on which the function is invoked
        :param exceptions: list of exceptions
        :param abstract: specifies whether function is abstract
        :return:
        """
        ...

    def generate_function_invocation(self, variable: Variable, function_name: str, params: [Variable]) -> str:
        """
        Function to invoke a function on the specified variable.

        :param variable: variable to invoke function of.
        :param function_name: function name to invoke.
        :param params: params to invoke the function with.
        :return:
        """
        ...

    def generate_if_clause(self, condition: str) -> str:
        """
        Function to generate an if clause for the given condition.

        :param condition:
        :return:
        """

    def generate_exception(self, exception_type: str, message: str) -> str:
        """
        Function to generate an exception in the given programming language.

        :param exception_type: type of the exception
        :param message:
        :return:
        """
        ...

    def generate_comment(self, message: str):
        """
        Function to generate a comment in the given programming language.

        :param message:
        :return:
        """
        ...

    def declare_variable(self, variable: Variable):
        """
        Function to declare a variable.

        :param variable: variable to declare
        :return:
        """

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

    def block_start_delimiter(self) -> str:
        """
        Function to retrieve the block start delimiter

        :return: block start delimiter of the programming language
        """
        ...

    def block_end_delimiter(self) -> str:
        """
        Function to retrieve the block start delimiter

        :return: block start delimiter of the programming language
        """
        ...

    def terminate_statement_delimiter(self) -> str:
        """
        Function to retrieve the block start delimiter

        :return: block start delimiter of the programming language
        """
        ...

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

    def generate_function_header(self, name: str, return_type: str, params: [Variable], exceptions: [str] = None, abstract: bool = False) -> str:
        exceptions_formatted = f" throws {', '.join([self.to_class_name(exception) for exception in exceptions])}" if exceptions is not None else ""
        end_formatted = ";" if abstract else ''
        return f"public {'abstract' if abstract else ''} {self.to_class_name(return_type)} {self.to_function_name(name)}({', '.join([param.type + ' ' + self.to_variable_name(param.name) for param in params])}){exceptions_formatted}{end_formatted}"

    def generate_function_invocation(self, variable: Variable, function_name: str, params: [Value]) -> str:
        return f"{self.unroll_getters(variable)}.{function_name}({', '.join(param.format_value(self) for param in params)});"

    def generate_if_clause(self, condition: str) -> str:
        return f"if({condition}"

    def generate_comment(self, message: str):
        return f"// {message}"

    def generate_exception(self, exception_type: str, message: str) -> str:
        return f'throw new {self.to_class_name(exception_type)}("{message}");'

    def declare_variable(self, variable: Variable):
        return f"{self.to_class_name(variable.type)} {self.to_variable_name(variable.name)} = new {self.to_class_name(variable.type)}();"

    def block_start_delimiter(self) -> str:
        return "{"

    def block_end_delimiter(self) -> str:
        return "}"

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
        # Limitation: Unable to determine long type in Python
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


class PythonSyntax(LanguageSyntax):

    def generate_if_clause(self, condition: str) -> str:
        return f"if {condition}"

    def generate_comment(self, message: str):
        return f"# {message}"

    def block_start_delimiter(self) -> str:
        return ":"

    def block_end_delimiter(self) -> str:
        return ""

    def generate_exception(self, exception_type: str, message: str) -> str:
        return f'raise {exception_type}("{message}");'