# Capstone Project - Airbnb neighbourhood popularity analysis based on data from Newyork City. 
Capstone project for Udacity Data Engineering Nanodegree course

## Objective:
Airbnb is a platform business that provides an interface between the hosts and the guests. Anybody with available space can become a host on Airbnb and offer it to global community. 

We will try to find out which neighborhood has the maximum airbnb listings, the total listing, available_listings on a date in the neighborhood and premium properties based on certain criterion in the neighborhood. 

### Steps taken in the project:
1. Downloaded dataset from kaggle to local machine.
2. Uploaded the the kaggle csv files from local machine to Amazon S3 bucket.
3. Used Apache Airflow to orchestrate  ETL process
4. Used python to structure ETL process and query redshift cluster.
5. Created the table structures on Redshift database.
6. Inserted the  data from csv file using copy command to respective staging tables i.e staging_listing,  staging_review, staging_calendar .
7. Populated dimensions from staging tables , while populating cleansing operation was performed.
8. Fact tables are populated from dimension tables for optimization and accuracy . Fact tables are produced using **snowflake schema methodology**.
9. In the end data quality checks were performed to ensure  data consistency and accuracy. 

### Data source Details:
The data is gathered from Kaggle open data catalog. Following datasets are in scope of this project:

 1. calendar.csv (source:
    https://www.kaggle.com/arthbr11/new-york-city-airbnb-open-data)
 2. listings.csv (source :
    https://www.kaggle.com/arthbr11/new-york-city-airbnb-open-data
 3. review.csv
    (source:https://www.kaggle.com/arthbr11/new-york-city-airbnb-open-data)

Sample datsets screenshots:
***Calendar***
![Calendar CSV Sample](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/Calendar.csv.png?raw=true)

***Listings***
![Listing CSV Sample](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/Listing.csv.png?raw=true)

***Review***
![Review CSV Sample](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/Review.csv.png?raw=true)

**Project Scope:**

The scope of this project is to create a data pipeline which will accept the source files, process and clean them, transform as per the the need of the final data model and load them in dimension which helps populating fact tables. We are going to read the source files from Amazon S3 service, using airflow and python to create a data pipeline, and eventually load the processed and transformed data into the data model created in Amazon Redshift database.

### Technology used:

 1. Apache Airflow : An open-source tool to programmatically author, 	schedule, and monitor workflows.
 2. Python : For ETL
 3. PostgreSQL : For ETL 
 4. Amazon Redshift : It is a fully-managed petabyte-scale cloud based data warehouse product designed for large scale data set storage and analysis . 
 5. Amazon S3 : It has a simple web services interface that you can use to store and retrieve any amount of data, at any time, from anywhere.
 6. Snowflake Schema : For optimizing table load and at the same time normalize dataset and reduce data redundancy  .
 
 ### Data Updatation 
The data needs to be updated on daily basis as we want to know the  listings availablity in the neighborhood on daily basis. It is possible that new listings will be added or some old listing might be discontinued.

**Project Data Model**

The data model consists of following 7 dimension tables and 2 fact tables:
***Dimension Tables***
 1. dim_host 
 2. dim_district
 3.  dim_neighbourhood 
 4. dim_listing
 5.  dim_review
 6. dim_property
 7. dim_date

***Fact Tables***
  1. fact_listing 
 2. fact_neighbourhood

***Who can use this dataset?***

The airbnb sales & marketing team can understand performace of listing within the neighbourhood for driving more sales and also help host to perform better which will help drive more sales. 

***Queries Answered By Each Table***

****fact_neighbourhood****

Fact table to measure neighbourhoods & stores metric such as total number of listings,number of available listings, price within neighbourhood such as min, max and avg price.
This helps end business user to measure neighbourhood performace and at the same time allows them to do comparision accross neighbourhoods and drive more sale in company.

****fact_listings****

Fact table to store metric for listings it helps to answer listing review count & rating count and whether listing is premium or not.

****dim_date****

Company's Calendar Tables , that's been used in day to day business.
 
****dim_listing****

Dimension table stores attributes related to listing and answers about price, availability with neighbourhood and district for a calendar date.

****dim_district****

Dimension table used to generate enterprise district_id for each district in newyork city.

****dim_host****

Dimension table to store host attribute and answer query specific to host identity ,about his listings, location, superhost etc.

****dim_property****

Dimension table stores property attributes and answers queries such as property type, who is host, room_type, bedrooms, bathrooms, amenities etc.

****dim_review****

Dimension table stores customer reviews & ratings for each listing on airbnb.

****dim_neighbourhood****

Dimension table stores mapping of meighbourhood in a district.


### Data-Model
![Data Model Diagram](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/capstone_data_model.png?raw=true)

### Data Pipeline Design
The data pipeline was designed using Apache Airflow. The whole process was segregated in several phases:

Creating the Staging tables
Loading the Staging tables
Creating the dimension tables
Loading the dimension tables
Creating the facts tables
Loading the fact tables
Performing data quality checks

**Airflow DAG**
![Airflow Dag](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/Capstone_Dag.png?raw=true)
Addressing Other Scenarios
The data was increased by 100x.
Amazon S3 is used and loading them using Spark would be a feasible choice.


The pipelines would run on a daily basis.
This can be handled using the existing Airflow DAG using the scheduling feature of Airflow.

The database needed to be accessed by 100+ people.
The number of nodes can be increased in the Amazon Redshift Cluster .

***Table-name : Fact_listing***

| Field_name|Data Type|Description|
|-------------|----------|--------|
| listing_id |integer          |	The unique id associated with each listing, Fk to dim_listing|	  
| neighbourhood_id |integer|  The unique id associated with each neighbourhood, Fk to dim_neighbourhood|
|  avg_rating      |decimal(10,2)      |The avg_review rating of the listing in the neighbourhood |
| review_count|integer| Total number of reviews on the listing |
|is_premium |char(1)|Flag is to identify the listing as premium listing. (expected values - 'Y' as yes and 'N' as no)

![Airflow Dag](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/fact_listing.png?raw=true)


***Table name -Fact_neighbourhood***

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|neighbourhood_id	|Integer|Neighbourhood Id. FK to dim_neighbourhood table|
|date	|date	|Calendar date|
|total_number_of_listings	|integer|	Total number of listngs for any neighbourhood on a calendar date|
|number_of_available_listing	|integer|	Total number of listing where available flag = 't'  for any neighbourhood on calendar date|
|number_of_unavailable_listing	|integer|	Total number of listing where available flag = 'y'  for any neighbourhood on calendar date|
|min_price	|numeric(18,2)|	Minimum price in neighbourhoods on calendar date|
|max_price	|numeric(18,2)|	Maximum price in neighbourhoods on calendar date|
|avg_price	|numeric(18,2)|	Average price in neighbourhoods on calendar date|

![Airflow Dag](https://github.com/swati0189/Udacity-Data-Engineering-Projects/blob/main/fact_neighbourhood.png?raw=true)


***Table name -dim_listing***	

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|listing_id |	integer	|Listing id. Primary key|
|Date	|date	|Date. Primary key|
|available	|char(1)	|Availibility of the listing for that given date. Expected values - 't' as true and 'f' as false|
|price |	numeric(18,2)	|Price of the listing on that given date|
|host_id	|integer	|Host id for that listing|
|district_id|integer| district_id for that listing, Fk to dim_district|
|neighbourhood_id| integer| neighbourhood_id for that listing, Fk to dim_neighbourhood table|

***Table name -dim_property***

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|listing_id	|integer	|Listing id. Primary key|
|host_id	|integer	|Host id for that listing. Primary key|
|property_type	|varchar(2000)	|Type of the property. Values are like -Entire apartment, Entire house, Private room in apartment etc.|
|room_type	|varchar(256)	|Type of rooms. Values are like - Private room, hotel room, shared room etc.|
|accomodates	|integer	|Number of people could stay in that property|
|bathrooms	|varchar(256)	|Number of bathrooms in that property. values are like-1 bath, 1 shared bath etc.|
|bedrooms	|integer	|number of beed rooms in that property|
|beds	|integer	|number of beds in that property|
|amenities	|varchar(2000)	|List of amenities for that property, like- air conditioning, microwave,hair dryer etc.|
|latitude	|numeric(18,2)	|Latitude of the property|
|longitude	|numeric(18,2)	|Longitude of the property|


***Table name -dim_host***	

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|host_id	|integer	|Host_id is Primary key|
|host_name	|varchar(256)	|Host name|
|host_is_superhost	|varchar(1)	|Indicator for superhost. Expected values - 't' as true and 'f' as false|
|host_url	|varchar(5000)	|URL for host information|
|host_since	|date	|The date since he is host |
|host_loaction	|varchar(1000)	|location of the host|
|host_has_profile_pic| varchar(1)| Expected values - 't' as true and 'f' as false|
| host_identity_verified| varchar(1)|Expected values - 't' as true and 'f' as false|
| room_type|varchar(256)|Type of room|
| host_is_superhost| varchar(1)|Expected values - 't' as true and 'f' as false|

***Table name -dim_date***	

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|id_date	|integer	|id of the date. Primary key|
|date_actual	|date	|actual date|
|day_name	|text	|name of the day|
|day_of_week	|integer	|day of week|
|day_of_month	|integer	|day of month|
|current_date_flag	|integer	|flag for current date|
|week_of_month	|integer	|Week of month|
|week_of_year	|integer	|week of year|
|month_number	|integer	|month number|
|month _name	|text	|month name|
|quarter_actual	|integer	|quarter|
|year_actual	|integer	|year|
|weekend_indicator	|text	|indicator for weekend|

***Table name -dim_district***

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|district_id	|integer	|District_id is Primary Key, Unique identifier |
|district	|varchar(256)	|District Name |


 ***Table name -dim_review***

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|review_id 	|integer	|review_id is Primary key|
|listing_id	|integer	|listing_id is Fk to dim_listing|
|review_scores_rating	|DECIMAL(5,2)|	review scores on the listing maximum is 5 star|
|number_of_reviews |INT| number of reviews on the listing|
|review_scores_accuracy| DECIMAL(5,2)|the score accuracy of review|
|review_scores_cleanliness| DECIMAL(5,2)| cleanliness score of the listing|
|review_scores_communication| DECIMAL(5,2)| communication score of the listing|
|review_scores_location| DECIMAL(5,1)| location score of the listing|
|review_scores_value| DECIMAL(5,1)| review_score_value of the listing|


***Table name -dim_neighbourhood***

|Field Name    | Data Type | Description                     |
|--------------|-----------|---------------------------------|
|neighbourhood_id	|integer	|Neighbourhood id is Primary key, Unique identifier|
|district_id	|integer	|District id, Fk to dim_district|
|neighbourhood	|varchar(256)|	Neighbourhood name


***Query and Output*** 

***To find total number of listings and , how much are available, on the given date across neighbourhoods in newyork***

```sql
select dn.neighbourhood_id, neighbourhood, total_number_of_listings, number_of_available_listing 
from fact_neighbourhood df
join dim_neighbourhood dn 
on df.neighborhood_id = dn.neighbourhood_id
where date= '2021--11-24'
limit 10
```


|neighbourhood_id|neighbourhood  |total_number_of_listings|number_of_available_listing|
|----------------|---------------|------------------------|---------------------------|
|               7|New Springville|                       4|                          2|
|              19|Port Richmond  |                       6|                          3|
|              59|Corona         |                      57|                         41|
|              63|Far Rockaway   |                      30|                         21|
|              67|Ozone Park     |                      52|                         30|
|              95|Little Neck    |                       4|                          3|
|             107|Flatbush       |                     439|                        141|
|             131|Bergen Beach   |                      10|                          8|
|             135|Carroll Gardens|                     180|                         54|
|             155|Tremont        |                      14|                          8|


***To find which neighbourhood has maximum number of listing on a given date in newyork*** 

```sql
select neighbourhood_id, neighbourhood,total_number_of_listings 
from 
(select dn.neighbourhood_id, neighbourhood, total_number_of_listings,
row_number() OVER( order by total_number_of_listings DESC) RNK
from fact_neighbourhood df
join dim_neighbourhood dn 
on df.neighborhood_id = dn.neighbourhood_id
where date= '2021--11-24') Z
where RNK =1 
```

|neighbourhood_id|neighbourhood     |total_number_of_listings|
|----------------|------------------|------------------------|
|             122|Bedford-Stuyvesant|                    2679|
