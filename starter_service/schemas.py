import json
import logging
from pathlib import Path
from pydoc import locate

from starter_service.avro_parser import avsc_to_pydantic

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
    _logger = logging.getLogger(__name__)
    _pathlib_path = None
    _schemas = {}

    @classmethod
    def get_schemas(cls):
        return cls._schemas

    @classmethod
    def get_schemas_dict(cls):
        return {schema.topic: schema.class_name for class_name, schema in cls._schemas.items()}

    @classmethod
    def initialize(cls, path=None):
        cls._pathlib_path = Path(path) if path else Path().absolute()
        cls._logger.info(f"Initializing SchemaRegistry {cls._pathlib_path}")
        # create schema folder if not exists
        cls._init_dir()
        # load schemas from folder
        cls._load_local_schemas()
        # load classes from folder
        cls._load_local_classes()

    @classmethod
    def _load_local_schemas(cls):
        # load avro schemas from local folder
        _schemas = []
        try:
            if not cls._pathlib_path.exists():
                cls._pathlib_path.mkdir()
        except Exception as e:
            cls._logger.error(f"Error creating schema folder: {e}")
            return

        schemas_dir = cls._pathlib_path / "schemas"
        for file in schemas_dir.iterdir():
            if file.suffix == ".avsc" or file.suffix == "-value.avsc" or file.suffix == "-key.avsc":
                _schemas.append(file)

        if len(_schemas) == 0:
            cls._logger.warning(f"No schemas found in {cls._pathlib_path.absolute()}")
            return

        for file in _schemas:
            cls._load_schema_from_file(file.name)

    @classmethod
    def _load_local_classes(cls):
        classes_dir = cls._pathlib_path / "classes"
        for file in classes_dir.iterdir():
            if file.suffix == ".py" and file.name != "__init__.py":
                topic = file.name.replace(".py", "")
                if topic in cls._schemas:
                    continue
                cls._logger.info(f"Loading class {topic} from file {file}")
                main_class = cls._read_main_class_from_file(file.name)
                schema = Schema(topic, file, main_class, cls._load_class_from_file(f'{topic}.py', main_class), file)
                cls._schemas[topic] = schema

    @classmethod
    def _read_main_class_from_file(cls, file):
        main_class = None
        _logger.info(f"Reading main class from file {file}")
        file_path = cls._pathlib_path / "classes" / file
        if not file_path.exists():
            _logger.error(f"File {file_path} does not exist")
            return None
        with open(file_path, "r") as f:
            for line in f.readlines():
                if line.startswith("main_class"):
                    main_class = line.split(" ")[2].strip()
                    break
            _logger.info(f"Main class is {main_class}")
        return main_class

    @classmethod
    def register_schema(cls, schema: [str, dict], topic: str):
        """
        Register a schema from a string or dict
        :param schema: AVRO string schema
        :param topic: topic to register the schema for
        :param _type: SchemaType Enum (consume or produce)
        :return:
        """
        cls._logger.info(f"Registering schema for topic {topic}")
        filename, main_class, python_classes = cls._avro_to_file(schema)

        full_path = cls._pathlib_path / "classes" / f'{topic}.py'
        cls._logger.info(f"Writing schema to file {full_path}, path {cls._pathlib_path}")
        with open(full_path, "w") as f:
            f.write(python_classes)
        schema = Schema(topic, filename, main_class, cls._load_class_from_file(f'{topic}.py', main_class), full_path)
        _logger.info(f"Registering schema {schema.__dict__} for topic {topic}")
        cls._schemas[topic] = schema

    @classmethod
    def _load_class_from_file(cls, filename, class_name):
        absolute = str(Path().absolute())
        path = str(cls._pathlib_path).replace(absolute, "").replace("/", ".")
        cls._logger.info(
            f"Loading class from file {filename} with class {class_name}, path {path}.classes.{filename[:-3]}.{class_name}")
        return locate(f"{path}.classes.{filename[:-3]}.{class_name}", True)

    @classmethod
    def _avro_to_file(cls, schema: [str, dict]) -> [str, str, str]:
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

    @classmethod
    def _load_schema_from_file(cls, filename):
        try:
            cls._logger.info(f"Loading schema {filename}")
            filepath = cls._pathlib_path / "schemas" / filename
            with open(filepath, "r") as f:
                cls.register_schema(f.read(), cls._filename_to_topic(filename))
        except Exception as e:
            cls._logger.error(f"Error loading schema {filename}: {e}")

    @classmethod
    def _filename_to_topic(cls, filename):
        if filename.endswith("-value.avsc"):
            return filename[:-11].lower()
        if filename.endswith("-key.avsc"):
            return filename[:-9].lower()
        if filename.endswith(".avsc"):
            return filename[:-5].lower()

    @classmethod
    def _init_dir(cls):
        try:
            if not cls._pathlib_path.exists():
                cls._pathlib_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            cls._logger.error(f"Error creating schema folder: {e}")
            return
        classes_dir = cls._pathlib_path / "classes"
        if not classes_dir.exists():
            try:
                classes_dir.mkdir()
                init_file = classes_dir / "__init__.py"
                init_file.touch()
                readme_file = classes_dir / "readme.txt"
                readme_file.write_text("This folder contains all the generated classes from the schemas")
            except Exception as e:
                cls._logger.error(f"Error creating classes folder {e}")
                raise e

        schemas_dir = cls._pathlib_path / "schemas"
        if not schemas_dir.exists():
            try:
                schemas_dir.mkdir()
                readme_file = schemas_dir / "readme.txt"
                readme_file.write_text("Use this folder to provide AVRO schemas")
            except Exception as e:
                cls._logger.error(f"Error creating schema folder {e}")
                raise e

    @classmethod
    def get_schema(cls, topic) -> object or dict:
        if not topic:
            return dict
        if topic not in cls._schemas.keys():
            return dict
        return cls._schemas[topic].class_obj
