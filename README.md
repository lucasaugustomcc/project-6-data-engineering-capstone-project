# US Immigration Analysis
### Data Engineering Capstone Project

#### Project Summary

The objective of this project was to create an ETL pipeline for I94 immigration and US demographics datasets to form an analytics database on immigration events. A use case for this analytics database is to find immigration patterns to the US. 

### The project follows the following steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Project Scope 
To create the analytics database, the following steps will be carried out:
* Use Spark to load the data into dataframes.
* Exploratory data analysis of I94 immigration dataset to identify missing values and strategies for data cleaning.
* Exploratory data analysis of demographics dataset to identify missing values and strategies for data cleaning.
* Perform data cleaning functions on all the datasets.
* Create dimension tables.
    * Create US demographics dimension table from the US cities demographics data. This table links to the fact table through the state code field.
    * Create fact table from the clean I94 immigration dataset and the ports dimension.

The technology used in this project is Apache Spark. Data will be read and staged from the customers repository using Spark.

#### Describe and Gather Data 
- *I94 Immigration Data:* This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This is where the data comes from](https://www.trade.gov/national-travel-and-tourism-office). The dataset used has 3 million rows. 

- *U.S. City Demographic Data*: This data comes from OpenSoft. [You can read more about it here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

#### Code

In addition to the data files, the project includes:

- etl.py - reads data from datasets, processes that data using Spark, and writes processed data as a set of dimensional tables into parquet files
- capstone.cfg - contains configuration that allows the ETL pipeline to access AWS EMR cluster.
- Jupyter Notebooks - jupyter notebook that was used for building the ETL pipeline.

## Step 2: Explore and Assess the Data

> Refer to the jupyter notebook for exploratory data analysis

## Step 3: Define the Data Model

### 3.1 Conceptual Data Model

### 3.2 Mapping Out Data Pipelines

The pipeline steps are as follows:

- Load the datasets
- Clean the I94 Immigration data to create Spark dataframe
- Clean demographics data
- Create ports dimension table
- Create immigration fact table
- Create demographic dimension table

## Step 4: Run Pipelines to Model the Data
### 4.1 Create the data model

#### Fact Table - data dictionary

| Feature               |Description                                           |
| ----------------------|:------------------------------------------------------| 
|cicid	                |Unique record ID|
|state_code	            |US state of arrival|
|entry_year             |4 digit year|
|entry_month            |Numeric month|
|port_code	            |Port of admission|
|arrival_date           |Arrival Date in the USA|
|departure_date         |Departure Date from the USA|
|age	                |Age of Respondent in Years|
|visa_type	            |Visa codes collapsed into three categories|
|birth_year	            |4 digit year of birth|
|gender	                |Non-immigrant sex |
|flight_number          |Flight Number |


#### US Demographics Dimension Table - data dictionary

| Feature               |Description                                           |
| ----------------------|:------------------------------------------------------|
|state_code 	        |US state code  |
|city 	                |City Name          |
|name 	                |City and US State code |
|median_age 	        |Median age of the population   |
|male_population 	    |Count of male population       |
|female_population 	    |Count of female population     |
|total_population 	    |Count of total population      |    
|number_of_veterans 	|Count of total Veterans        |
|foreign_born 	        |Count of residents of the city that were not born in the city |
|average_household_size |	Average city household size |
|race 	                |Respondent race                |
|count 	                |Count of city's individual per race |


### 4.2 Running the ETL pipeline

> python etl.py

## Step 5: Complete Project Write Up

* Rationale for the choice of tools and technologies for the project
    * Apache spark was used because of:
        * it's ability to handle multiple file formats with large amounts of data.
        * Apache Spark offers a lightning-fast unified analytics engine for big data.
        * Spark has easy-to-use APIs for operating on large datasets
    * Propose how often the data should be updated and why.
       * The current I94 immigration data is updated monthly, and hence the data will be updated monthly.
    * Write a description of how you would approach the problem differently under the following scenarios:
        * The data was increased by 100x.
            * Spark can handle the increase but we would consider increasing the number of nodes in our cluster.
        * The data populates a dashboard that must be updated on a daily basis by 7am every day.
            * In this scenario, Apache Airflow will be used to schedule and run data pipelines.
        * The database needed to be accessed by 100+ people.
            * In this scenario, we would move our analytics database into Amazon Redshift.