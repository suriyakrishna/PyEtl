from utils.args_parser import get_args
from utils.json_parser import get_application_arguments
from utils.etl_logging import pyetl_logger
from datetime import datetime
# from utils.common import time_difference
from utils.etl_processor import *
from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder.appName("PyEtl").master("yarn").getOrCreate()

def main():
    # Define logger for main
    logger = pyetl_logger("PyEtl")
    
    logger.info("#" * 75)
    logger.info(" " * 30 + "PyEtl Started" + " " * 30)
    logger.info("#" * 75)
    
    # Application Start time
    start_time = datetime.now()

    # Parser Input Arguments
    config = get_args()

    # Parser JSON File
    app_args = get_application_arguments(config.config_file_location)

    # Spark Session
    spark = get_spark_session()

    # Output Root Directory
    logger.info("Output Root Directory: {}".format(app_args.output_dir))
    # List All Sources
    logger.info("-" * 75)
    logger.info("Total number of Sources for Spark SQL temporary table: {}".format(len(app_args.extract)))
    for source in app_args.extract:
        logger.info(source)
    # List All Transformations
    logger.info("-" * 75)
    if(app_args.transform is not None):
        logger.info("Total number of transform for Spark SQL temporary table: {}".format(len(app_args.transform)))
        for sql in app_args.transform:
            logger.info(sql)
        logger.info("-" * 75)
    # List All Loads
    logger.info("Total number of Load SQL's: {}".format(len(app_args.load)))
    for sql in app_args.load:
        logger.info(sql)
    logger.info("#" * 75)
    
    logger.info("#" * 75)
    logger.info(" " * 12 + "Creating SPARK SQL Temporary table for ALL Sources" + " " * 12)
    logger.info("#" * 75)
    create_temp_table_from_sources(app_args.extract, spark)
    logger.info("#" * 75)

    logger.info("#" * 75)
    logger.info(" " * 20 + "Writing Result to Output Directory" + " " * 20)
    logger.info("#" * 75)
    execute_all_sqls(app_args.load, app_args.output_dir, spark)
    logger.info("#" * 75)

    # Application End Time
    end_time = datetime.now()

    #Time took
    # total_time = time_difference(start_time, end_time)

    logger.info("#" * 75)
    logger.info(" " * 25 + "Time Took: {}".format("NEED TO DO WORK AROUND") + " " * 25)
    logger.info("#" * 75)

if __name__ == '__main__':
    main()
    


