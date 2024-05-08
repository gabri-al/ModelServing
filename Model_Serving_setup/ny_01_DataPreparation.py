# Databricks notebook source
# MAGIC %md
# MAGIC # AirBnb NY Listing Price Prediction: Data Preparation

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
# MAGIC * Remove invalid availabilities (e.g., 0 or >365)

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

# Save into silver table
silver_step1 = spark.createDataFrame(silver_step1).createOrReplaceTempView("silver_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_data;
# MAGIC CREATE TABLE silver_data AS
# MAGIC   select * from silver_v;

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Gold data] Finalize data preparation for ML

# COMMAND ----------

# MAGIC %md
# MAGIC Steps required:
# MAGIC
# MAGIC * Remove uninformative columns (same value on all rows or different values on every row, columns with >50% Null values)
# MAGIC * Remove NAs
# MAGIC * Encode required categorical columns
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-Remove Uninformative Columns & NAs

# COMMAND ----------

gold1 = spark.sql("SELECT * from silver_data").toPandas()
display(gold1.info())

# COMMAND ----------

gold1.describe(percentiles=[.25, .5, .75])

# COMMAND ----------

# Drop columns
gold_step1 = gold1.copy()
to_drop = ['name','host_id','house_rules','license','host_name', ## Too many unique values
           'last_review', 'reviews_per_month',  ## Too many null values
           'country','country_code', ## Too few unique values
           'service_fee' ## Unlikely this info is available when we predict the price
           ]
for c in to_drop:
  gold_step1 = gold_step1.drop(c, axis=1)

display(gold_step1.info())

# COMMAND ----------

# Drop NAs
gold_step2 = gold_step1.copy()
for c in list(gold_step2.columns):
  len_pre = len(gold_step2)
  gold_step2.dropna(axis=0, subset=c, inplace=True)
  len_post = len(gold_step2)
  if len_pre>len_post:
    print(":: On col %s, removed %4d NAs" % (c, len_pre - len_post))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-Encode Categorical Columns

# COMMAND ----------

## Check values per column
for col in gold_step2.columns:
  if gold_step2[col].dtype == 'object' and col != 'id':
    gold_groupby = gold_step2.groupby(col).agg(records=('price','count'), avg_price=('price', 'mean'), std_price=('price', 'std'))
    gold_groupby.reset_index(inplace=True)
    print(":: %s has %2d unique values" % (col, len(gold_groupby)))
    gold_groupby.sort_values(by=col, inplace=True)
    display(gold_groupby)
    print("\n")

# COMMAND ----------

## Encode column with few (<10) unique values, drop the others
gold_step3 = spark.createDataFrame(gold_step2).createOrReplaceTempView("gold_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold_data;
# MAGIC CREATE TABLE gold_data AS
# MAGIC Select 
# MAGIC   id
# MAGIC   ,cast(case when host_identity_verified = 'verified' then 1 else 0 end as float) as is_host_identity_verified
# MAGIC   /* Neighborhood groups = Bronx, Brooklyn, Manhattan, Queens, Staten Island */
# MAGIC   ,cast(case when neighbourhood_group = 'Bronx' then 1 else 0 end as float) is_neighbourhood_group_Bronx
# MAGIC   ,cast(case when neighbourhood_group = 'Brooklyn' then 1 else 0 end as float) is_neighbourhood_group_Brooklyn
# MAGIC   ,cast(case when neighbourhood_group = 'Manhattan' then 1 else 0 end as float) is_neighbourhood_group_Manhattan
# MAGIC   ,cast(case when neighbourhood_group = 'Queens' then 1 else 0 end as float) is_neighbourhood_group_Queens
# MAGIC   /* dropping neighbourhood column */
# MAGIC   ,cast(lat as float)
# MAGIC   ,cast(long as float)
# MAGIC   ,cast(case when instant_bookable = 'TRUE' then 1 else 0 end as float) is_instant_bookable
# MAGIC   /* cancellation_policy = flexible, moderate, strict */
# MAGIC   ,cast(case when cancellation_policy = 'flexible' then 1 else 0 end as float) is_cancellation_policy_flexible
# MAGIC   ,cast(case when cancellation_policy = 'strict' then 1 else 0 end as float) is_cancellation_policy_strict
# MAGIC   /* room_type = Entire home/apt, Hotel room, Private room, Shared room */
# MAGIC   ,cast(case when room_type = 'Entire home/apt' then 1 else 0 end as float) is_room_type_Entire
# MAGIC   ,cast(case when room_type = 'Private room' then 1 else 0 end as float) is_room_type_Privateroom
# MAGIC   ,cast(case when room_type = 'Shared room' then 1 else 0 end as float) is_room_type_Sharedroom
# MAGIC   ,cast(construction_year as float) construction_year
# MAGIC   ,cast(minimum_nights as float) minimum_nights
# MAGIC   ,cast(number_of_reviews as float) number_of_reviews
# MAGIC   ,cast(review_rate_number as float) review_rate_number
# MAGIC   ,cast(calculated_host_listings_count as float) calculated_host_listings_count
# MAGIC   ,cast(availability_365 as float) availability_365
# MAGIC   ,cast(price as float) price
# MAGIC from gold_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from gold_data limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final counts

# COMMAND ----------

display(spark.sql("select count(*) from bronze_data;"))
display(spark.sql("select count(*) from silver_data;"))
display(spark.sql("select count(*) from gold_data;"))
