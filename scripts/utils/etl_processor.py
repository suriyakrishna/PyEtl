from utils.common import throw_error
from utils.json_parser import get_value
from utils.etl_logging import pyetl_logger
import sys

# Method to create Spark Sql Temp for Each Source
def create_temp_table_from_definition(source_definition, spark):
    temp_table_name = get_value(source_definition, "ALIAS").upper()
    if(temp_table_name is None):
      throw_error("ALIAS key is mandatory for all the sources")
    source_type = get_value(source_definition, "TYPE").upper()
    logger.info("CREATING SPARK SQL TEMP TABLE: {}".format(temp_table_name))
    logger.info("SOURCE TYPE: {}".format(source_type))
    if source_type == "FILE":
        file_format = get_value(source_definition, "FORMAT").upper()
        file_location = get_value(source_definition, "LOCATION")
        file_options = get_value(source_definition, "OPTIONS")
        if file_format is None or file_location is None:
            throw_error("For Source Type 'FILE'. FORMAT and LOCATION keys are mandatory")
        logger.info("FILE FORMAT: {}".format(file_format))
        logger.info("FILE LOCATION: {}".format(file_location))
        if file_format == "CSV":
            header_exists = get_value(file_options, "HEADER").lower()
            separator = get_value(file_options, "SEP")
            if header_exists is None or separator is None:
                throw_error("For File Format 'CSV'. HEADER and SEP keys are mandatory")
            logger.info("FILE OPTIONS: {}".format(file_options))
            df = spark.read.options(header = header_exists, delimited = separator).format(file_format).load(file_location)
            df.createOrReplaceTempView(temp_table_name)
        else:
            df = spark.read.format(file_format).load(file_location)
            df.createOrReplaceTempView(temp_table_name)
    elif source_type == "QUERY":
        query = get_value(source_definition, "QUERY")
        if query is None:
            throw_error("For Source Type 'QUERY'. QUERY key is mandatory")
        logger.info("QUERY - {}".format(query))
        df = spark.sql(query)
        df.createOrReplaceTempView(temp_table_name)
    elif source_type == "TABLE":
        table = get_value(source_definition, "TABLE")
        if table is None:
            throw_error("For Source Type 'TABLE'. TABLE key is mandatory")
        logger.info("TABLE: {}".format(table))
        df = spark.table(table)
        df.createOrReplaceTempView(temp_table_name)
    logger.info("TEMP TABLE: {} Created Successfully".format(temp_table_name))


def create_temp_table_from_sources(sources, spark):
  for source in sources:
    try:
      create_temp_table_from_definition(source, spark)
      if(sources[-1] != source):
        logger.info("-" * 75)
    except Exception as e:
      logger.error("Caught Exception while creating SPARK SQL Temporary table. Error: {}".format(e))
      sys.exit(1)


# Method to determine output type csv or json
def output_file_type(dataframe):
  fields = dataframe.schema.fields
  for field in fields:
    if field.dataType.typeName() == "array":
      return "json"
  return "csv"


# Method to write results to output_directory
def sql_to_result(sql, output_dir, spark):
  alias = get_value(sql, "ALIAS")
  query = get_value(sql, 'QUERY')
  logger.info("Executing Query: {}".format(query))
  if alias is None or query is None:
    throw_error("Each SQL Object should have it's own ALIAS and QUERY keys")
  df = spark.sql(query)
  result_format = output_file_type(df)
  result_dir = output_dir + "/" + alias
  logger.info("Writing results to output directory: {}, Format: {}".format(result_dir, result_format))
  if result_format == "csv":
    df.repartition(1).write.mode("overwrite").option("header", "true").format(result_format).save(result_dir) # need to implement repartition configured from config file default 1
  else:
    df.repartition(1).write.mode("overwrite").format(result_format).save(result_dir)
  logger.info("Data loaded Successfully")


def execute_all_sqls(sql_list, output_dir, spark):
  for sql in sql_list:
    try:
      sql_to_result(sql, output_dir, spark)
      if(sql_list[-1] != sql):
        logger.info("-" * 75)
    except Exception as e:
      logger.error("Caught Exception while writing results. Error: {}".format(e))
      sys.exit(1)

logger = pyetl_logger(__name__)