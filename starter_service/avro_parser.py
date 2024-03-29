import json
from typing import Optional, Union

_reserved_keywords = ["def", "class", "from", "to", "import", "as", "pass", "return", "raise", "try", "except",
                      "finally", "while", "for", "in", "continue", "break", "if", "elif", "else", "assert", "del",
                      "global", "nonlocal", "lambda", "with", "yield", "async", "await", "True", "False", "None", "and",
                      "or", "not", "is", "in", "schema"]


def avsc_to_pydantic(schema: dict) -> [str, str]:
    """Generate python code of pydantic of given Avro Schema"""
    if "type" not in schema or schema["type"] != "record":
        raise AttributeError("Type not supported")
    if "name" not in schema:
        raise AttributeError("Name is required")
    if "fields" not in schema:
        raise AttributeError("fields are required")

    classes = {}
    main_class = schema["name"]

    def get_python_type(t: Union[str, dict]) -> str:
        """Returns python type for given avro type"""
        optional = False
        union = False
        if isinstance(t, str):
            if t == "string":
                py_type = "str"
            elif t == "long" or t == "int":
                py_type = "int"
            elif t == "boolean":
                py_type = "bool"
            elif t == "double" or t == "float":
                py_type = "float"
            elif t == "null" or t == "None":
                py_type = "None"
            elif t in classes:
                py_type = t
            else:
                raise NotImplementedError(f"Type {t} not supported yet")
        elif isinstance(t, list):
            if "null" in t or "None" in t or None in t:
                optional = True
            if len(t) > 2 or (not optional and len(t) > 1):
                union = True
                py_type = [get_python_type(i) for i in t]
                # raise NotImplementedError("Only a single type ia supported yet")
            else:
                c = t.copy()
                if "null" in c:
                    c.remove("null")
                py_type = get_python_type(c[0])
        elif t.get("logicalType") == "uuid":
            py_type = "UUID"
        elif t.get("logicalType") == "decimal":
            py_type = "Decimal"
        elif t.get("logicalType") == "timestamp-millis" or t.get("logicalType") == "timestamp-micros":
            py_type = "datetime"
        elif t.get("logicalType") == "time-millis" or t.get("logicalType") == "time-micros":
            py_type = "time"
        elif t.get("logicalType") == "date":
            py_type = "date"
        elif t.get("type") == "enum":
            enum_name = t.get("name")
            if enum_name not in classes:
                enum_class = f"class {enum_name}(str, Enum):\n"
                for s in t.get("symbols"):
                    enum_class += f'    {s} = "{s}"\n'
                classes[enum_name] = enum_class
            py_type = enum_name
        elif t.get("type") == "string":
            py_type = "str"
        elif t.get("type") == "array":
            sub_type = get_python_type(t.get("items"))
            py_type = f"List[{sub_type}]"
        elif t.get("type") == "record":
            record_type_to_pydantic(t)
            py_type = t.get("name")
        elif t.get("type") == "map":
            value_type = get_python_type(t.get("values"))
            py_type = f"Dict[str, {value_type}]"
        else:
            raise NotImplementedError(
                f"Type {t} not supported yet, "
                f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
            )
        if optional:
            if isinstance(py_type, list):
                return f"Optional[Union[{', '.join(py_type)}]]"
            else:
                return f"Optional[{py_type}]"
        elif union:
            return f"Union[{', '.join(py_type)}]"
        else:
            return py_type

    def record_type_to_pydantic(schema: dict):
        """Convert a single avro record type to a pydantic class"""
        name = schema["name"]
        current = f"class {name}(BaseModel):\n"

        for field in schema["fields"]:
            n = field["name"]
            t = get_python_type(field["type"])
            default = field.get("default")
            if n in _reserved_keywords:
                n = f"{n}_"
            if "default" not in field:
                current += f"    {n}: {t}\n"
            elif isinstance(default, (bool, type(None))):
                current += f"    {n}: {t} = {default}\n"
            else:
                current += f"    {n}: {t} = {json.dumps(default)}\n"
        if len(schema["fields"]) == 0:
            current += "    pass\n"

        classes[name] = current

    record_type_to_pydantic(schema)

    file_content = """
import json
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Union
from uuid import UUID

from pydantic import BaseModel

"""
    file_content += "\n\n".join(classes.values())
    file_content += """
    def dict(self, *args, **kwargs):
            return {
                k[:-1] if k.endswith("_") else k: v for k, v in self.__dict__.items()
            }

    def json(self, *args, **kwargs):
            return json.dumps(self.dict(), *args, **kwargs)
    """
    file_content += f"\n\nmain_class = {main_class}\n"
    return file_content, main_class


def convert_file(avsc_path: str, output_path: Optional[str] = None):
    with open(avsc_path, "r") as fh:
        avsc_dict = json.load(fh)
    file_content = avsc_to_pydantic(avsc_dict)
    if output_path is None:
        print(file_content)
    else:
        with open(output_path, "w") as fh:
            fh.write(file_content)
