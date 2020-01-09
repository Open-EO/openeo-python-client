import json
import textwrap
import openeo

PROCESS_PATH = "filter_bbox.json"
OPENAPI_PATH = "openapi.json"

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

    def __init__(self):
        self.description = ""
        self.schema = None

    def __str__(self):
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
        if self.description:
            return "  :return: {}".format(textwrap.fill(self.description.replace("\n", ""), 80))
        else:
            return ""


class Parameter:

    def __init__(self, name):
        self.name = name
        self.type = None
        self.required = False
        self.req_prop = []
        self.schema = None
        self.default = ""
        self.description = ""

    def get_header_str(self):
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
        return "  :param {}: {}".format(self.name, textwrap.fill(self.description.replace("\n", ""), 80))


class Function:

    def __init__(self):
        self.description = ""
        self.summary = ""
        self.name = ""
        self.parameters = {}
        self.returntype = Result()

    def get_param(self):
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
        for param in param_list:
            self.parameters[param] = Parameter(name=param)

    def set_returntype(self, result: Result):
        self.returntype = result

    def get_parameters_doc_str(self):
        doc_text = ""
        for _, p_val in self.parameters.items():
            doc_text += p_val.get_doc_str() + "\n"
        return doc_text

    def __str__(self):
        # function header
        #if self.returntype:
        #    text = "def {} {}: -> '{}'\n".format(self.name, self.get_param(), self.returntype)
        #else:
        text = "def {}{}:\n".format(self.name, self.get_param())

        # docstring
        text += "    \"\"\"{}\n{}\n{}\n\"\"\"".format(textwrap.fill(self.description.replace("\n", ""), 80),
                                                            self.get_parameters_doc_str(),
                                                            self.returntype.get_doc_str())

        # function body
        text += "\n    {}".format(self.get_function_body())

        return text

    def get_function_body(self):

        args = "{"

        for param, val in self.parameters.items():
            if args == "{":
                if param == "data":
                    args += '"data": {"from_node": from_node},'
                else:
                    args += '"{}": {},'.format(param, param)
            else:
                if param == "data":
                    args += ' "data": {"from_node": from_node},'
                else:
                    args += ' "{}": {},'.format(param, param)

        args = args[:-1]+"}"

        if args == "}":
            args = "None"

        return "return cube.graph_add_process('{}', {})".format(self.name, args)


class ProcessParser:

    def parse_processes_connection(self, connection):

        func_list = []

        process_list = connection.list_processes()

        for process in process_list:
            func_list.append(self.parse_process(process))

        return func_list

    def parse_process_list(self, file_path):

        func_list = []

        with open(file_path) as json_file:
            process_list = json.load(json_file)

        if "processes" in process_list:
            for process in process_list['processes']:
                func_list.append(self.parse_process(process))

        return func_list

    def parse_process_json(self, file_path):
        with open(file_path) as json_file:
            process = json.load(json_file)

        func = self.parse_process(process)
        return func

    def parse_process(self, process):

        func = Function()

        # Parse process and generate functioncode out of it
        for key, val in process.items():
            # print(key)
            if not key in FUNCTION_MAPPING:
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
                    # print("{} --> {}".format(str(p_key), str(p_val)))
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
                func.set_returntype(result)
        return func

PROCESS_LIST_PATH = "processes_GEE.json"

pp = ProcessParser()

GEE_DRIVER_URL = "https://earthengine.openeo.org/v0.4"
USER = "group8"
PASSWD = "test123"

con = openeo.connect(GEE_DRIVER_URL)
con.authenticate_basic(username=USER, password=PASSWD)

func_list = pp.parse_processes_connection(con)
#func_list = pp.parse_process_list(PROCESS_LIST_PATH)

text = ""
for func in func_list:
    text += str(func)+"\n\n\n"

text_file = open("processes.py", "w")
text_file.write(text)
text_file.close()