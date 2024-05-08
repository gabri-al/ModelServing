# Databricks notebook source
# MAGIC %md
# MAGIC # AirBnb NY Listing Price Prediction ~ Data Preparation

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog, Schema Creation

# COMMAND ----------

catalog_ = f"price_prediction"
schema_ = f"ny_listing"

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS "+catalog_)
spark.sql("CREATE SCHEMA IF NOT EXISTS "+catalog_+"."+schema_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Ingestion from Volume [Bronze]

# COMMAND ----------

spark.sql("USE CATALOG "+catalog_)
spark.sql("USE SCHEMA "+schema_)

# COMMAND ----------

path_ = f"dbfs:/Volumes/"+catalog_+"/"+schema_+"/ny_listing/NY_Listing_cleaned.csv"
bronze_ = (spark.
           read.
           option("header", True).
           #option("delimiter", ",").
           option("encoding", "utf-8").
           csv(path_).
           createOrReplaceTempView("bronze_v"))

display(spark.sql("SELECT * FROM bronze_v LIMIT 10;"))


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_data;
# MAGIC CREATE TABLE bronze_data AS
# MAGIC   SELECT id, NAME as name, `host id` as host_id, host_identity_verified, `host name` as host_name, `neighbourhood group` as neighbourhood_group
# MAGIC     ,neighbourhood, lat, long, country,`country code` as country_code, instant_bookable, cancellation_policy, `room type` as room_type,`Construction year` as construction_year, price, `service fee` as service_fee, `minimum nights` as minimum_nights, `number of reviews` as number_of_reviews, `last review` as last_review, `reviews per month` as reviews_per_month, `review rate number` as review_rate_number,`calculated host listings count` as calculated_host_listings_count, `availability 365` as availability_365, house_rules, license
# MAGIC   FROM bronze_v;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning [Silver]

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning steps required:
# MAGIC * Remove duplicates
# MAGIC * Remove invalid characters from price column (e.g., $ ,)
# MAGIC * Remove columns with >50% Null values
# MAGIC * Remove columns with same value on all rows or different values on every row
# MAGIC * Remove invalid availabilities (e.g., 0 or >365)
# MAGIC * Remove rows containing nulls

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-Remove Duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicates
# MAGIC with d0 (
# MAGIC select distinct * from bronze_data)
# MAGIC select "Tot Records" as count, count(*) value_ from bronze_data
# MAGIC   union select "Unique Records" as count, count(*) value_ from d0;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS silver_data_1;
# MAGIC CREATE TEMPORARY VIEW silver_data_1 AS
# MAGIC with d0 as (
# MAGIC   select distinct *
# MAGIC   from bronze_data
# MAGIC   where (host_identity_verified in ('verified', 'unconfirmed') or host_identity_verified is null) 
# MAGIC   and availability_365 is not null
# MAGIC   and id not in (' Laundry in building  8 mints from A C trains')
# MAGIC   and construction_year is not null
# MAGIC   and minimum_nights is not null and minimum_nights not like '$%'
# MAGIC   and number_of_reviews is not null and number_of_reviews not like '$%'
# MAGIC   and review_rate_number is not null and review_rate_number not like '$%' and review_rate_number not like '%/%'
# MAGIC   and calculated_host_listings_count is not null and calculated_host_listings_count not like '%.%'
# MAGIC   -- Removing a few problematic data
# MAGIC )
# MAGIC   select id,	`name`,	host_id,	host_identity_verified,	host_name
# MAGIC   ,case when neighbourhood_group = 'brookln' then 'Brooklyn' when neighbourhood_group = 'manhatan' then 'Manhattan' else neighbourhood_group end as neighbourhood_group
# MAGIC   ,	neighbourhood,	lat,	long,	country,	country_code, instant_bookable, cancellation_policy, room_type, construction_year, price, service_fee
# MAGIC   ,cast(case when abs(minimum_nights)>365 then 365 else abs(minimum_nights) end as int) as minimum_nights
# MAGIC   , number_of_reviews, last_review,	reviews_per_month,	review_rate_number, calculated_host_listings_count
# MAGIC   ,cast(case when abs(availability_365)>365 then 365 else abs(availability_365) end as int) as availability_365
# MAGIC   , house_rules, license
# MAGIC   from d0;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-Convert Column Formats

# COMMAND ----------

silver_step1 = spark.sql("SELECT * from silver_data_1").toPandas()

# COMMAND ----------

# Int columns
silver_step1['id'] = silver_step1['id'].astype(int)
silver_step1['host_id'] = silver_step1['host_id'].astype(int)
silver_step1['construction_year'] = silver_step1['construction_year'].astype(int)
silver_step1['minimum_nights'] = silver_step1['minimum_nights'].astype(int)
silver_step1['number_of_reviews'] = silver_step1['number_of_reviews'].astype(int)
silver_step1['review_rate_number'] = silver_step1['review_rate_number'].astype(int)
silver_step1['calculated_host_listings_count'] = silver_step1['calculated_host_listings_count'].astype(int)
silver_step1['availability_365'] = silver_step1['availability_365'].astype(int)

#float
silver_step1['lat'] = silver_step1['lat'].astype(float)
silver_step1['long'] = silver_step1['long'].astype(float)
silver_step1['reviews_per_month'] = silver_step1['reviews_per_month'].astype(float)

#date
silver_step1['last_review'] = pd.to_datetime(silver_step1['last_review'])

# Cleaning and float
silver_step1['service_fee'] = silver_step1['service_fee'].str.replace('$', '')
silver_step1['service_fee'] = silver_step1['service_fee'].str.replace(',', '')
silver_step1['service_fee'] = silver_step1['service_fee'].astype(float)

silver_step1['price'] = silver_step1['price'].str.replace('$', '')
silver_step1['price'] = silver_step1['price'].str.replace(',', '')
silver_step1['price'] = silver_step1['price'].astype(float)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-Remove Uninformative Columns

# COMMAND ----------

display(silver_step1.info())

# COMMAND ----------

silver_step1.describe(percentiles=[.25, .5, .75])

# COMMAND ----------

# Drop columns
silver_step2 = silver_step1.copy()
to_drop = ['id','name','host_id','house_rules','license','host_name', ## Too many unique values
           'last_review', 'reviews_per_month',  ## Too many null values
           'country','country_code' ## Too few unique values
           ]
for c in to_drop:
  silver_step2 = silver_step2.drop(c, axis=1)

display(silver_step2.info())

# COMMAND ----------

# Drop NAs
silver_step3 = silver_step2.copy()
for c in list(silver_step3.columns):
  len_pre = len(silver_step3)
  silver_step3.dropna(axis=0, subset=c, inplace=True)
  len_post = len(silver_step3)
  if len_pre>len_post:
    print(":: On col %s, removed %4d NAs" % (c, len_pre - len_post))

# COMMAND ----------

# Save into silver table
silver_step4 = spark.createDataFrame(silver_step3).createOrReplaceTempView("silver_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_data;
# MAGIC CREATE TABLE silver_data AS
# MAGIC   select * from silver_v;

# COMMAND ----------

display(spark.sql("select count(*) from bronze_data;"))
display(spark.sql("select count(*) from silver_data;"))
