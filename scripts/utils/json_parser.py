# from json import JSONDecodeError
import json
from utils.etl_logging import pyetl_logger
from utils.common import throw_error

class application_config:
    def __init__(self, extract, transform, load, output_dir):
        self.extract = extract
        self.transform = transform
        self.load = load
        self.output_dir = output_dir

def get_value(json_object, key):
    if key in json_object:
        return json_object[key]
    else:
        return None

def read_config_file(file_path):
    config = {}
    try:
        with open(file_path, 'r') as conf:
            try:
                config = json.load(conf)
            except Exception as e:
                logger.error("JSON Decode Error: {}".format(e))
                throw_error("JSON Decode Error.. Exiting")
    except Exception as f:
        logger.error("File Not Found Exception: {}".format(f))
        throw_error("File not found. Exiting..")
    return config

def get_application_arguments(file_path):
    logger.info("Parsing input JSON config file: {}".format(file_path))
    config = read_config_file(file_path)
    extract = get_value(config, 'SOURCE')
    transform = get_value(config, 'TRANSFORM')
    load = get_value(config, 'LOAD')
    output_dir = get_value(config, 'OUTPUT_DIRECTORY')
    if (output_dir is None or extract is None or load is None):
        throw_error("MISSING Required KEYS in JSON. OUTPUT_DIRECTORY, SOURCE or LOAD key is missing in JSON config file")
    logger.info("Parsed JSON file successfully.")
    return application_config(extract, transform, load, output_dir)

logger = pyetl_logger(__name__)