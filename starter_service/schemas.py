import json
import logging
from pathlib import Path
from pydoc import locate
from threading import Lock

from starter_service.avro_parser import avsc_to_pydantic

_pathlib_path = Path().absolute()
_logger = logging.getLogger(__name__)


class Schema:

    def __init__(self, topic=None, filename=None, class_name=None, class_obj=None, full_path=None):
        self.topic = topic
        self.filename = filename
        self.class_name = class_name
        self.class_obj = class_obj
        self.full_path = full_path

    def __str__(self):
        return f"{self.class_name} from {self.filename}"

    def __repr__(self):
        return f"{self.class_name} from {self.filename}"


class SchemaRegistry:
    _lock = Lock()
    _instance = None
    _logger = logging.getLogger(__name__)

    _schemas = {}
    _classes = {}

    def __new__(self):
        with self._lock:
            if self._instance is None:
                self._instance = super().__new__(self)
        return self._instance

    def get_schemas(self):
        return self._schemas

    def get_schemas_dict(self):
        return {schema.topic: schema.class_name for class_name, schema in self._schemas.items()}

    def initialize(self):
        # create schema folder if not exists
        self._init_dir()
        # load schemas from folder
        self._load_local_schemas()
        # load classes from folder
        self._load_local_classes()

    def _load_local_schemas(self):
        # load avro schemas from local folder
        _schemas = []
        schemas_dir = _pathlib_path / "schemas"
        for file in schemas_dir.iterdir():
            if file.suffix == ".avsc" or file.suffix == "-value.avsc" or file.suffix == "-key.avsc":
                _schemas.append(file)

        if len(_schemas) == 0:
            self._logger.warning(f"No schemas found in {_pathlib_path.absolute()}")
            return

        for file in _schemas:
            self._load_schema_from_file(file.name)

    def _load_local_classes(self):
        classes_dir = _pathlib_path / "classes"
        for file in classes_dir.iterdir():
            if file.suffix == ".py" and file.name != "__init__.py":
                topic = file.name.replace(".py", "")
                if topic in self._schemas:
                    continue
                self._logger.info(f"Loading class {topic} from file {file}")
                main_class = self._read_main_class_from_file(file)
                schema = Schema(topic, file, main_class, self._load_class_from_file(f'{topic}.py', main_class), file)
                self._schemas[topic] = schema

    def _read_main_class_from_file(self, file):
        main_class = None
        file_path = _pathlib_path / "classes" / file
        with open(file_path, "r") as f:
            for line in f.readlines():
                if line.startswith("main_class"):
                    main_class = line.split(" ")[2].strip()
        return main_class

    def register_schema(self, schema: [str, dict], topic: str):
        """
        Register a schema from a string or dict
        :param schema: AVRO string schema
        :param topic: topic to register the schema for
        :param _type: SchemaType Enum (consume or produce)
        :return:
        """
        self._logger.info(f"Registering schema for topic {topic}")
        filename, main_class, python_classes = self._avro_to_file(schema)

        full_path = _pathlib_path / "classes" / f'{topic}.py'
        with open(full_path, "w") as f:
            f.write(python_classes)
        schema = Schema(topic, filename, main_class, self._load_class_from_file(f'{topic}.py', main_class), full_path)
        self._schemas[topic] = schema

    def _load_class_from_file(self, filename, class_name):
        self._logger.info(f"Loading class from file {filename} with class {class_name}")
        return locate(f"classes.{filename[:-3]}.{class_name}")

    def _avro_to_file(self, schema: [str, dict]) -> [str, str, str]:
        """
        :param schema: AVRO schema as string or dict
        :return: filename, main_class, python_classes
        """
        if isinstance(schema, str):
            schema = json.loads(schema)
        class_name = schema['name']
        if class_name is None:
            raise ValueError(f"Schema {schema[:20]} does not contain a name")
        python_classes, main_class = avsc_to_pydantic(schema)
        filename = f"{class_name.lower()}.py"
        return filename, main_class, python_classes

    def _load_schema_from_file(self, filename):
        try:
            self._logger.info(f"Loading schema {filename}")
            filepath = _pathlib_path / "schemas" / filename
            with open(filepath, "r") as f:
                self.register_schema(f.read(), self._filename_to_topic(filename))
        except Exception as e:
            self._logger.error(f"Error loading schema {filename}: {e}")

    def _filename_to_topic(self, filename):
        if filename.endswith("-value.avsc"):
            return filename[:-11].lower()
        if filename.endswith("-key.avsc"):
            return filename[:-9].lower()
        if filename.endswith(".avsc"):
            return filename[:-5].lower()

    def _init_dir(self):
        classes_dir = _pathlib_path / "classes"
        if not classes_dir.exists():
            try:
                classes_dir.mkdir()
                init_file = classes_dir / "__init__.py"
                init_file.touch()
                readme_file = classes_dir / "readme.txt"
                readme_file.write_text("This folder contains all the generated classes from the schemas")
            except Exception as e:
                self._logger.error(f"Error creating classes folder {e}")
                raise e

        schemas_dir = _pathlib_path / "schemas"
        if not schemas_dir.exists():
            try:
                schemas_dir.mkdir()
                readme_file = schemas_dir / "readme.txt"
                readme_file.write_text("Use this folder to provide AVRO schemas")
            except Exception as e:
                self._logger.error(f"Error creating schema folder {e}")
                raise e

    def get_schema(self, topic) -> object or dict:
        with self._lock:
            if not topic or topic not in self._schemas:
                return dict
            return self._schemas[topic].class_obj


SchemaRegistry().initialize()
