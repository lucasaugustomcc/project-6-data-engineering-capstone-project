import configparser
import logging

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, Row
# Set logging config
logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')

I94_DATA_DIR = config['DATA']['I94_DATA_DIR']
DEMOGRAPHICS_DATA_FILE = config['DATA']['DEMOGRAPHICS_DATA_FILE']
SAS_DESCRIPTION_FILE = config['DATA']['SAS_DESCRIPTION_FILE']
OUTPUT_DATA_DIR = config['DATA']['OUTPUT_DATA_DIR']

def main():
    spark = get_spark_session()

    logger.info("Loading datasets into Spark dataframes")
    immigration_df = get_immigration_data(spark)
    demographics_df = get_demographics_data(spark)
    logger.info("Datasets loaded.")

    logger.info("Loading dimensional data into Spark dataframes")
    ports_df = get_dimension_data(spark,'I94PORT')
    logger.info("Dimensional data loaded.")

    logger.info("Cleaning data")
    ports_df = clean_ports_data(ports_df)
    logger.info("Data cleaned")

    logger.info("Checking data quality")
    logger.info("Data quality checked")
    
    logger.info("Creating tables")
    fact_table_df = create_immigration_fact_table(spark, immigration_df, ports_df)
    logger.info("Fact table created")

    demographics_table = create_demographics_dim_table(spark, demographics_df, ports_df)
    logger.info("Demographics dimension table created")

    logger.info("Tables created")

    # Save tables in parquet format
    logger.info("Writing data in parquet format")

    fact_table_df.write.mode('overwrite').partitionBy('entry_year', 'entry_month', 'port_code').parquet(
        OUTPUT_DATA_DIR + "fact_immigrations.parquet")
    demographics_table.write.mode('overwrite').partitionBy('state_code').parquet(
        OUTPUT_DATA_DIR + "dim_city_demographics.parquet")

    logger.info("Writing data is done.")

def create_demographics_dim_table(spark, demographics_df, ports_df):
    
    demographics_df.createOrReplaceTempView('staging_us_demographics')
    ports_df.createOrReplaceTempView('staging_ports')

    aggregated_df = spark.sql("""
            SELECT
                sud.city,
                sud.state_code,
                SUM(sud.male_population) AS male_population,
                SUM(sud.female_population) AS female_population,
                SUM(sud.total_population) AS total_population,
                SUM(sud.number_of_veterans) AS number_of_veterans,
                SUM(sud.foreign_born) AS num_foreign_born
            FROM staging_us_demographics sud
            GROUP BY sud.city, sud.state_code
        """)

    aggregated_df.createOrReplaceTempView('combined_demographics')
    return spark.sql("""
            SELECT
                sp.code AS port_code,
                cd.*
            FROM staging_ports sp
                JOIN combined_demographics cd 
                    ON lower(cd.city) = lower(sp.city) AND cd.state_code = sp.state_code
        """)

def clean_demographics_data(demographics_df):
    logger.info("Clean demographics dataset.")
    return demographics_df \
        .dropDuplicates()

def clean_ports_data(ports_df):
    logger.info("Clean ports data.")
    
    return ports_df \
        .dropna() \
        .dropDuplicates()

def get_immigration_data(spark):
    logger.info("Reading immigration data")
    return spark.read.parquet(I94_DATA_DIR)

def get_demographics_data(spark):
    schema = StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", IntegerType()),
        StructField("female_population", IntegerType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("foreign_born", IntegerType()),
        StructField("average_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType())
    ])
    logger.info("Reading demographics data")
    return spark.read.csv(DEMOGRAPHICS_DATA_FILE, sep=';', header=True, schema=schema)

def get_dimension_data(spark, label):
    sas_data = get_data_from_SAS_description_file(label)
    df = pd.DataFrame(data=sas_data,columns=['code','name'])

    df[['city', 'state_code']] = \
        df['name'].str.rsplit(',', 1, expand=True)

    df.to_csv('output/file.csv', index=False)
    return spark.read.csv('file.csv', header=True)

def create_immigration_fact_table(spark, immigration_df, ports_df):
    immigration_df.createOrReplaceTempView('staging_immigration_data')    
    ports_df.createOrReplaceTempView('staging_ports')

    return spark.sql("""
            SELECT
                sid.cicid AS cicid,
                sid.i94yr AS entry_year,
                sid.i94mon AS entry_month,                
                sp.code AS port_code,
                sid.arrdate AS arrival_date,
                sid.depdate AS departure_date,
                sid.i94bir AS age,
                sid.occup AS occupation,
                sid.gender AS gender,
                sid.biryear AS birth_year,
                sid.dtaddto AS entry_date,
                sid.airline AS airline,
                sid.admnum AS admission_number,
                sid.fltno AS flight_number,
                sid.visatype AS visa_type
            FROM staging_immigration_data sid
                LEFT JOIN staging_ports sp ON sp.code = sid.i94port
            WHERE 
                sp.code IS NOT NULL
        """)

def get_data_from_SAS_description_file(label):
    
    with open(SAS_DESCRIPTION_FILE) as file:
        file_data = file.read()

    label_data = file_data[file_data.index(label):]
    label_data = label_data[:label_data.index(';')]

    lines = label_data.split('\n')
    code_value_list = []
    for line in lines:
        line = line.split('=')
        if len(line) != 2:
            continue
        code = line[0].strip().strip("'")
        value = line[1].strip("'").strip().strip("'").strip()
        code_value_list.append((code, value, ))

    return code_value_list

def get_spark_session():
    """Create spark session."""

    logger.info("Creating Spark session")
    spark = SparkSession.builder \
        .appName("Capstone Project") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    logger.info("Spark session created")
    return spark

if __name__ == '__main__':
    main()