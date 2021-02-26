from abc import ABC
from variable import *


class LanguageSyntax(ABC):
    """
    Abstract class to define the syntax for a programming language.
    """
    def get_file_extension(self) -> str:
        """
        Function to retrieve the file extension for given programming language.
        :return:
        """
        ...

    def unroll_getters(self, variable: Variable) -> str:
        """
        Function to unroll the getters applied on the variable.

        :param variable: variable to unroll getters
        :return:
        """

    def generate_import(self, module: str, dependency: str):
        """
        Function to generate an import statement for the specified dependency.

        :param module:
        :param str:
        :param dependency:
        :return:
        """
        ...

    def generate_class(self, name: str, parent_classes: [] = None, abstract: bool = False, final: bool = False) -> str:
        """
        Function to generate a class in the programming language.

        :param name: name of the class
        :param parent_classes: names of parent classes
        :param abstract: indicator whether class is abstract
        :return:
        """
        ...

    def generate_function_header(self, name: str, return_type: str, params: [Variable], exceptions: [str] = None, abstract: bool = False, static: bool = False) -> str:
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

    def generate_return(self, value: Value) -> str:
        """
        Function to generate a return statement

        :param value:
        :return:
        """
        ...

    def generate_comment(self, message: str) -> str:
        """
        Function to generate a comment in the given programming language.

        :param message:
        :return:
        """
        ...

    def declare_variable(self, variable: Variable) -> str:
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

    @staticmethod
    def underscore_to_camelcase(s: str) -> str:
        return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)


class JavaSyntax(LanguageSyntax):
    """
    Class that defines the syntax of the Java programming language.
    """
    def get_file_extension(self) -> str:
        return "java"

    def unroll_getters(self, variable: Variable) -> str:
        # TODO Unsure if this is the right location to do this, maybe move to variable
        return self.to_variable_name(variable.name) + "".join([self.underscore_to_camelcase(f".get_{getter.field_name}()") for getter in variable.getters])

    def generate_import(self, module: str, dependency: str):
        return f"import {module}.{dependency};"

    def generate_class(self, name: str, parent_classes: [] = None, abstract: bool = False, final: bool = False) -> str:
        parents_formatted = f" extends {', '.join([self.to_class_name(clss) for clss in parent_classes])}" if parent_classes is not None else ""
        return f"public{' abstract' if abstract else ''}{' final' if final else ''} class {self.to_class_name(name)}{parents_formatted} " + '{'

    def generate_function_header(self, name: str, return_type: str, params: [Variable], exceptions: [str] = None, abstract: bool = False, static: bool = False) -> str:
        exceptions_formatted = f" throws {', '.join([self.to_class_name(exception).capitalize() for exception in exceptions])}" if exceptions is not None else ""
        end_formatted = ";" if abstract else ' '
<<<<<<< HEAD
        return f"public{' static ' if static else '' }{' abstract' if abstract else ''} {self.to_class_name(return_type)} {self.to_function_name(name)}({', '.join([param.type + ' ' + self.to_variable_name(param.name) for param in params])}){exceptions_formatted}{end_formatted}"
=======
        return f"public {' static ' if static else '' } {' abstract' if abstract else ''} {self.to_class_name(return_type)} {self.to_function_name(name)}({', '.join([param.type + ' ' + self.to_variable_name(param.name) for param in params])}){exceptions_formatted}{end_formatted}"
>>>>>>> 425959e9ba6aa246d29360a1dbc8a7e404a87e40

    def generate_function_invocation(self, variable: Variable, function_name: str, params: [Value]) -> str:
        return f"{self.unroll_getters(variable)}.{function_name}({', '.join(param.format_value(self) for param in params)});"

    def generate_if_clause(self, condition: str) -> str:
        return f"if({condition})"

    def generate_return(self, value: Value) -> str:
        return f"return {value.format_value(self)}"

    def generate_comment(self, message: str):
        return f"// {message}"

    def generate_exception(self, exception_type: str, message: str) -> str:
        return f'throw new {self.to_class_name(exception_type)}("{message}");'

    def declare_variable(self, variable: Variable) -> str:
        return f"{self.to_class_name(variable.type)} {self.to_variable_name(variable.name)} = new {self.to_class_name(variable.type)}();"

    def block_start_delimiter(self) -> str:
        return "{"

    def block_end_delimiter(self) -> str:
        return "}"

    def terminate_statement_delimiter(self) -> str:
        return ";"

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

        if isinstance(val, list):
            return f"Arrays.asList({', '.join([self.format_constant_value(item) for item in val])})"

        if isinstance(val, str):
            return f'"{val}"'

        if isinstance(val, float):
            return f'{val}f'

        return val


class PythonSyntax(LanguageSyntax):

    def get_file_extension(self) -> str:
        return "py"

    def unroll_getters(self, variable: Variable) -> str:
        return variable.name + "".join([self.underscore_to_camelcase(f".{getter.field_name}") for getter in variable.getters])

    def generate_import(self, module: str, dependency: str):
        return f"from {module} import {dependency}"

    def generate_class(self, name: str, parent_classes: [] = None, abstract: bool = False, final: bool = False) -> str:
        parents_formatted = f"({', '.join([self.to_class_name(clss) for clss in parent_classes])})" if parent_classes is not None else ""
        return f"class {self.to_class_name(name)}{parents_formatted}"

    def generate_function_header(self, name: str, return_type: str, params: [Variable], exceptions: [str] = None, abstract: bool = False, static: bool = False) -> str:
        return f"def {self.to_function_name(name)}(self{''.join([f', {self.to_variable_name(param.name)}: {param.type}' for param in params])}) -> {return_type}:"

    def generate_function_invocation(self, variable: Variable, function_name: str, params: [Value]) -> str:
        return f"{self.unroll_getters(variable)}.{function_name}({', '.join(param.format_value(self) for param in params)});"

    def generate_if_clause(self, condition: str) -> str:
        return f"if {condition}"

    def generate_comment(self, message: str):
        return f"# {message}"

    def generate_exception(self, exception_type: str, message: str) -> str:
        return f"raise {self.to_class_name(exception_type)}({self.format_constant_value(message)})"

    def declare_variable(self, variable: Variable) -> str:
        return f"{self.to_variable_name(variable.name)} = {self.to_class_name(variable.type)}()"

    def block_start_delimiter(self) -> str:
        return ":"

    def block_end_delimiter(self) -> str:
        return ""

    def terminate_statement_delimiter(self) -> str:
        return ""

    def to_function_name(self, name: str) -> str:
        return name

    def to_variable_name(self, name: str) -> str:
        return name

    def to_class_name(self, name: str) -> str:
        # Implements upperCamelCase
        if len(name) > 0:
            return self.underscore_to_camelcase(name)
        return name

    def format_constant_value(self, val: object) -> str:
        # Limitation: Unable to determine long type in Python
        if val is None:
            return "None"

        if isinstance(val, str):
            return f'"{val}"'

        if isinstance(val, float):
            return f'{val}f'

        return val
