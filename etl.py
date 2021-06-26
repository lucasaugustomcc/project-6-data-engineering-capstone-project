import configparser
import logging

from pyspark.sql import SparkSession

# Set logging config
logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')

I94_DATA_DIR = config['DATA']['I94_DATA_DIR']
DEMOGRAPHICS_DATA_FILE = config['DATA']['DEMOGRAPHICS_DATA_FILE']
OUTPUT_DATA_DIR = config['DATA']['OUTPUT_DATA_DIR']

def main():
    spark = get_spark_session()
    immigration_df = get_immigration_data()
    demographics_df = get_demographics_data()

def get_immigration_data(spark):
    logger.info("Reading immigration data")
    return spark.read.parquet(I94_DATA_DIR)

def get_demographics_data(spark):
    logger.info("Reading demographics data")
    return spark.read.parquet(DEMOGRAPHICS_DATA_FILE)

def get_spark_session():
    """Create spark session."""
    spark = SparkSession.builder \
        .appName("Capstone Project") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    logger.info("Spark session created")
    return spark

if __name__ == '__main__':
    main()