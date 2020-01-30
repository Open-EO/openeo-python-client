"""
Processes library generation tool. Standalone tool to generate a python module out of the /processes endpoint of a
backend. The library can then be used with the DataCube object of the python client interchangeable with static
processes of the client.
"""

import json
import textwrap
import openeo
import keyword
import argparse
import sys
import os
from pathlib import Path
import datetime

# Mappings of the openEO API formats to the python formats

FUNCTION_MAPPING = {
    "id": "function_name",
    "description": "description",
    "summary": "summary",
    "parameter_order": "parameter_order",
    "parameters": "parameters",
    "returns": "returns"
}

PARAMETER_MAPPING = {
    "description": "description",
    "schema": "schema",
    "required": "required"
}

SCHEMA_MAPPING = {
    "type": "type",
    "properties": "properties",
    "required": "required"
}

NATIVE_TYPES_MAPPING = {
    'int': "int",
    'long': "int",
    'float': "float",
    'str': "str",
    'bool': "bool",
    'date': "datetime.date",
    'datetime': "datetime.datetime",
    'object': "dict",
    'raster-cube': 'raster-cube'
}


class Result:
    """Class representing an the result handling of the loaded process.
       It generates the return statement of the generated function.
    """
    def __init__(self):
        self.description = ""
        self.schema = None

    def __str__(self):
        """
                Gives the Python type of the openEO process result.

                :return: String of the Python type of the result.
        """
        type_str = ""
        if "type" in self.schema:
            if not isinstance(self.schema["type"], list):
                if self.schema["type"] in NATIVE_TYPES_MAPPING:
                    type_str = NATIVE_TYPES_MAPPING[self.schema["type"]]
            else:
                if self.schema["type"][0] in NATIVE_TYPES_MAPPING:
                    type_str = NATIVE_TYPES_MAPPING[self.schema["type"][0]]
        if "subtype" in self.schema:
            if not isinstance(self.schema["subtype"], list):
                if self.schema["subtype"] in NATIVE_TYPES_MAPPING:
                    type_str = NATIVE_TYPES_MAPPING[self.schema["subtype"]]
            else:
                if self.schema["subtype"][0] in NATIVE_TYPES_MAPPING:
                    type_str = NATIVE_TYPES_MAPPING[self.schema["subtype"][0]]
        return type_str

    def get_doc_str(self):
        """
                Returns the doc string of the openEO result, empty string if the Result description is empty.

                :return: Doc string of the result.
        """
        if self.description:
            return "  :return: {}".format(textwrap.fill(self.description.replace("\n", ""), 80))
        else:
            return ""


class Parameter:
    """Class representing one parameter of the loaded process.
       It generates the function parameter line of the generated function definition
       as well as the doc string for the parameter.
    """
    def __init__(self, name):
        self.name = name
        self.type = None
        self.required = False
        self.req_prop = []
        self.schema = None
        self.default = ""
        self.description = ""

    def get_header_str(self):
        """
            Returns the string in the function definition for the parameter

            :return: string of the parameter in the function header.
        """
        if self.type:
            if self.required:
                return "{}: {}".format(self.name, self.type)
            else:
                if self.default:
                    return "{}={}".format(self.name, self.default)
                else:
                    return "{}=None".format(self.name)
        else:
            return str(self.name)

    def get_doc_str(self):
        """
            Returns the doc string of the openEO parameter, empty string if the Parameter description is empty.

            :return: Doc string of the parameter.
        """
        return "  :param {}: {}".format(self.name, textwrap.fill(self.description.replace("\n", ""), 80))


class Function:
    """Class representing the generated function of the loaded process.
       It contains the Parameter objects and the Result object of the process.
       It is responsible of writing the function header, body and return statement, as well as the doc string.
    """
    def __init__(self):
        self.description = ""
        self.summary = ""
        self.name = ""
        self.parameters = {}
        self.returntype = Result()

    def get_param(self):
        """
            Combines all existing parameters and returns the complete parameter string of the function header.

            :return: String of the function parameters.
        """
        parameters = ("cube", )
        has_data = False
        for _, param in self.parameters.items():
            if param.name != "data":
                parameters += (param.get_header_str(), )
            else:
                has_data = True

        if has_data:
            parameters += ("from_node=None",)
        param_str = str(parameters)

        if len(parameters) == 1:
            param_str = param_str.replace(",", "")

        return param_str.replace("\"", "").replace("'", "")

    def set_ordered_param(self, param_list):
        """
            Defines for all parameters in param_list a Parameter object and stores it in the parameters attribute.

            :param param_list: list of parameter names (string)
        """
        for param in param_list:
            self.parameters[param] = Parameter(name=param)

    def get_parameters_doc_str(self):
        """
            Reads from all Parameter objects of parameters the generated doc string.

            :return: String of all parameter doc strings of the function
        """
        doc_text = ""
        for _, p_val in self.parameters.items():
            doc_text += p_val.get_doc_str() + "\n"
        return doc_text

    def __str__(self):
        """
            Generates the string of the whole Python function out of this function object. Therefore, using the
            description, the parameters and the result information of the loaded process.

            :return: String of the whole Python function definition.
        """
        # Check if function name is a keyword for Python
        if self.name in keyword.kwlist:
            self.name = "_{}".format(self.name)
        text = "def {}{}:\n".format(self.name, self.get_param())

        # docstring
        text += textwrap.indent("\"\"\"{}\n{}\n{}\n\"\"\"".format(textwrap.fill(self.description.replace("\n", ""), 80),
                                                                  self.get_parameters_doc_str(),
                                                                  self.returntype.get_doc_str()), "    ")

        # function body
        text += "\n{}".format(self.get_function_body())

        return text

    def get_function_body(self):
        """
            Generates the string for this functions body. If there is a parameter named "data" it assumes,
            that a datacube needs to be provided. It tries to apply smoothly to the current python client to allow a
            datacube (imagecollection) object as input and by default uses the last node of it as previous node.
            To add the process seemlessly into the existing datacube, the graph_add_process method is used.


            :return: String of the Python function body.
        """
        arguments = "{"
        for param, val in self.parameters.items():
            if arguments == "{":
                if param == "data":
                    arguments += '"data": {"from_node": from_node},'
                else:
                    arguments += '"{}": {},'.format(param, param)
            else:
                if param == "data":
                    arguments += ' "data": {"from_node": from_node},'
                else:
                    arguments += ' "{}": {},'.format(param, param)

        arguments = arguments[:-1]+"}"

        if arguments == "}":
            arguments = "None"

        code_str = ""
        # Default: Setting from_node to the previous
        if "from_node" in arguments:
            code_str = "    if not from_node:\n        from_node = cube.node_id\n"

        return code_str+"    return cube.graph_add_process('{}', {})".format(self.name, arguments)


class ProcessParser:
    """Class of process parser, is used to parse the JSON from the /processes endpoint to generate for each process a
       function and write this to a separate newly created python file. This then can be imported to use all processes
       the backend provides.
    """

    def __init__(self, connection=None, json_file=None, output_path=None):
        self.connection = connection
        self.json_file = json_file
        self.output_path = output_path
        self.func_list = []

    def get_file_header(self):
        """
            Returns the file header comment containing the creation time, the path to the program that generated it and
            the input used for the generation.

            :return: string of the file header comment.
        """
        file_path = Path(__file__).absolute()
        cur_time = datetime.datetime.now()
        source = None

        if self.connection:
            source = self.connection.build_url("")
        elif self.json_file:
            source = self.json_file

        return "\"\"\"\nThis file was generated using {} \nInput: {}\nCreated: {} \n\"\"\"\n\n\n".format(file_path,
                                                                                                         source,
                                                                                                         cur_time)

    def write_processes_to_file(self, func_list=None, path=None):
        """
            Writes the current function list as generated Python functions into the currently defined output path with
            the file name of "processes.py". If the parameters func_list or path are set, the function uses these values
            instead of the object internal ones.


            :param func_list: list of Function objects
            :param path: path where the Python file should be stored (string)
        """
        if not func_list:
            func_list = self.func_list
        if not path:
            path = self.output_path

        text = self.get_file_header()
        for func in func_list:
            text += str(func) + "\n\n\n"

        if path:
            path = os.path.join(path, "processes.py")
        else:
            path = "processes.py"
        text_file = open(path, "w")
        text_file.write(text)
        text_file.close()

    def parse_processes_connection(self, connection=None):
        """
            Parses the processes JSON from the /processes endpoint of the given connection. If the connection parameter
            is not set, it uses the object internal one.

            :param connection: The openEO connection object.

            :return: list of Function objects.
        """
        if not connection:
            connection = self.connection
        else:
            self.connection = connection

        f_list = []

        process_list = connection.list_processes()

        for process in process_list:
            f_list.append(self.parse_process(process))

        self.func_list = f_list
        return f_list

    def parse_process_file(self, file_path=None):
        """
            Parses the processes JSON from a JSON file. If the file_path parameter
            is not set, it uses the object internal one.

            :param file_path: Path to the processes JSON file.

            :return: list of Function objects.
        """
        if not file_path:
            file_path = self.json_file
        else:
            self.json_file = file_path

        f_list = []

        with open(file_path) as json_file:
            process_list = json.load(json_file)

        if "processes" in process_list:
            for process in process_list['processes']:
                f_list.append(self.parse_process(process))

        self.func_list = f_list
        return f_list

    def parse_single_process_json(self, file_path=None):
        """
            Generates a function out of a single process JSON file.

            :param file_path: Path to the process JSON file.

            :return: Function object.
        """
        if not file_path:
            file_path = self.json_file

        with open(file_path) as json_file:
            processes = json.load(json_file)

        func = self.parse_process(processes)
        return func

    def parse_process(self, process):
        """
            Parses a process JSON object and generates a Function object out of it.

            :param process: dict of a process (JSON from the backend).

            :return: Function object.
        """
        func = Function()

        # Parse process and generate function code out of it
        for key, val in process.items():
            # print(key)
            if key not in FUNCTION_MAPPING:
                continue
            elif FUNCTION_MAPPING[key] == "function_name":
                func.name = str(val)
            if FUNCTION_MAPPING[key] == "parameter_order":
                func.set_ordered_param(val)
            if FUNCTION_MAPPING[key] == "summary":
                func.summary = val
            if FUNCTION_MAPPING[key] == "description":
                func.description = val
            if FUNCTION_MAPPING[key] == "parameters":
                for p_key, p_val in val.items():
                    if p_key not in func.parameters:
                        func.parameters[p_key] = Parameter(name=p_key)
                    if "required" in p_val:
                        func.parameters[p_key].required = p_val["required"]
                    if "description" in p_val:
                        func.parameters[p_key].description = p_val["description"]
                    if "schema" in p_val:
                        if "type" in p_val["schema"]:
                            if not isinstance(p_val["schema"]["type"], list):
                                if p_val["schema"]["type"] in NATIVE_TYPES_MAPPING:
                                    func.parameters[p_key].type = NATIVE_TYPES_MAPPING[p_val["schema"]["type"]]
                            else:
                                if p_val["schema"]["type"][0] in NATIVE_TYPES_MAPPING:
                                    func.parameters[p_key].type = NATIVE_TYPES_MAPPING[p_val["schema"]["type"][0]]
                        if "subtype" in p_val["schema"]:
                            if p_val["schema"]["subtype"] in NATIVE_TYPES_MAPPING:
                                func.parameters[p_key].type = NATIVE_TYPES_MAPPING[p_val["schema"]["subtype"]]
                        if "required" in p_val["schema"]:
                            func.parameters[p_key].req_prop = p_val["schema"]["required"]
                        if "properties" in p_val["schema"]:
                            func.parameters[p_key].schema = p_val["schema"]["properties"]

            if FUNCTION_MAPPING[key] == "returns":
                result = Result()
                if "description" in val:
                    result.description = val["description"]
                if "schema" in val:
                    result.schema = val["schema"]
                func.returntype = result
        return func


# Command line interface if it is called directly.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Processes generation tool for the python client.')
    parser.add_argument('--url', dest='url', default=None,
                        help='Url of the backend, the processes should be '
                             'generated from, if it is not read from a JSON file')
    parser.add_argument('--input_file', dest='input_file', default=None,
                        help='JSON file, the processes should be read from, if it is not read from an url')
    parser.add_argument('--output_path', dest='output_path', default="",
                        help='Path of where the processes.py should be generated to (default:current directory)')

    args = parser.parse_args()

    if not bool(args.url) != bool(args.input_file):
        print("Either the --url parameter or the input_file parameter have to be set, but not both!")
        sys.exit(-1)

    pp = ProcessParser()

    if args.url:
        con = openeo.connect(args.url)
        pp.parse_processes_connection(con)
    else:
        pp.parse_process_file(args.input_file)

    pp.write_processes_to_file(path=args.output_path)
