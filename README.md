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
    * Create immigration calendar dimension table from I94 immigration dataset, this table links to the fact table through the arrdate field.
    * Create country dimension table from the I94 immigration and the global temperatures dataset. The global land temperatures data was aggregated at country level. The table links to the fact table through the country of residence code allowing analysts to understand correlation between country of residence and immigration to US states.
    * Create US demographics dimension table from the US cities demographics data. This table links to the fact table through the state code field.
    * Create fact table from the clean I94 immigration dataset and the visa_type dimension.

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

### 3.2 Mapping Out Data Pipelines

The pipeline steps are as follows:

- Load the datasets
- Clean the I94 Immigration data to create Spark dataframe for each month
- Create visa_type dimension table
- Create country dimension table
- Create immigration fact table
- Load demographics data
- Clean demographics data
- Create demographic dimension table

## Step 4: Run Pipelines to Model the Data
### 4.1 Create the data model

Fact Table - data dictionary

| Feature               |Description                                           |
| ----------------------|:------------------------------------------------------| 
|record_id	            | Unique record ID|
|country_residence_code	|3 digit code for immigrant country of residence|
|visa_type_key	        |A numerical key that links to the visa_type dimension table |
|state_code	            |US state of arrival|
|i94yr	                |4 digit year|
|i94mon	                |Numeric month|
|i94port	            |Port of admission|
|arrdate	            |Arrival Date in the USA|
|i94mode	            |Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)|
|i94addr	            |USA State of arrival|
|depdate	            |Departure Date from the USA|
|i94bir	                |Age of Respondent in Years|
|i94visa	            |Visa codes collapsed into three categories|
|count	                |Field used for summary statistics|
|dtadfile	            |Character Date Field - Date added to I-94 Files|
|visapost	            |Department of State where where Visa was issued|
|occup	                |Occupation that will be performed in U.S|
|entdepa	            |Arrival Flag - admitted or paroled into the U.S.|
|entdepd	            |Departure Flag - Departed, lost I-94 or is deceased|
|entdepu	            |Update Flag - Either apprehended, overstayed, adjusted to perm residence
|matflag	            |Match flag - Match of arrival and departure records|
|biryear	            |4 digit year of birth|
|dtaddto	            |Character Date Field - Date to which admitted to U.S. (allowed to stay until)|
|gender	                |Non-immigrant sex |


US Demographics Dimension Table - data dictionary

The whole of this dataset comes from the us cities demographics data.
| Feature               |Description                                           |
| ----------------------|:------------------------------------------------------|
|id 	                |Record id                      |
|state_code 	        |US state code  |
|City 	                |City Name          |
|State 	                |US State where city is located |
|Median Age 	        |Median age of the population   |
|Male Population 	    |Count of male population       |
|Female Population 	    |Count of female population     |
|Total Population 	    |Count of total population      |    
|Number of Veterans 	|Count of total Veterans        |
|Foreign born 	        |Count of residents of the city that were not born in the city |
|Average Household Size |	Average city household size |
|Race 	                |Respondent race                |
|Count 	                |Count of city's individual per race |


### 4.2 Running the ETL pipeline


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