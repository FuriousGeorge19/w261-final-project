# Databricks notebook source
# MAGIC %md
# MAGIC ### Init Script 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Without variables filled in, to highlight what's contained in them

# COMMAND ----------

from pyspark.sql.functions import col, max

blob_container = "" # The name of your container created in https://portal.azure.com
storage_account = "" # The name of your Storage account created in https://portal.azure.com
secret_scope = "" # The name of the scope created in your local computer using the Databricks CLI
secret_key = "" # The name of the secret key created in your local computer using the Databricks CLI 
blob_url = f"wasbs://{blob_container}@{storage_account}.blob.core.windows.net"
mount_path = "/mnt/mids-w261"

# COMMAND ----------

from pyspark.sql.functions import col, max

blob_container = "w261-team-9" # The name of your container created in https://portal.azure.com
storage_account = "w261team9" # The name of your Storage account created in https://portal.azure.com
secret_scope = "w261team9" # The name of the scope created in your local computer using the Databricks CLI
secret_key = "w261team9key" # The name of the secret key created in your local computer using the Databricks CLI 
blob_url = f"wasbs://{blob_container}@{storage_account}.blob.core.windows.net"
mount_path = "/mnt/mids-w261"

# COMMAND ----------

blob_url

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAS Token

# COMMAND ----------

spark.conf.set(
  f"fs.azure.sas.{blob_container}.{storage_account}.blob.core.windows.net",
  dbutils.secrets.get(scope = secret_scope, key = secret_key)
)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Import pyspark sql functions as F

# COMMAND ----------

> from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test it!
# MAGIC A *Read Only* mount has been made available to all clusters in this Databricks Platform. It contains data you will use for **HW5** and **Final Project**. Feel free to explore the files by running the cell below.

# COMMAND ----------

display(dbutils.fs.ls(f"{mount_path}"))

# COMMAND ----------

display(dbutils.fs.ls(f"{mount_path}/datasets_final_project/weather_data/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read/List current contents of our blob

# COMMAND ----------

display(dbutils.fs.ls(f"{blob_url}"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read 1 day of weather data (I think just 1 day)

# COMMAND ----------

# Load the Jan 1st, 2015 for Weather
df_weather = spark.read.parquet(f"{mount_path}/datasets_final_project/weather_data/*").filter(col('DATE') < "2015-01-02T00:00:00.000").cache()
display(df_weather)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Write `df_weather` to a folder in the blob called `weather_data_1d`

# COMMAND ----------

# This command will write to your Cloud Storage if right permissions are in place. 
# Navigate back to your Storage account in https://portal.azure.com, to inspect the files.
df_weather.write.parquet(f"{blob_url}/weather_data_1d")

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at what we just wrote

# COMMAND ----------

display(dbutils.fs.ls(f"{blob_url}/weather_data_1d"))

# COMMAND ----------

# MAGIC %md
# MAGIC Read what we just wrote back into a dataframe

# COMMAND ----------

# Load it the previous DF as a new DF
df_weather_new = spark.read.parquet(f"{blob_url}/weather_data_1d")
display(df_weather_new)

# COMMAND ----------

# MAGIC %md 
# MAGIC Look at the number of rows and the largest data in the dataframe (*should be 2015-01-01 considering what we chose earlier*)

# COMMAND ----------

print(f"Your new df_weather has {df_weather_new.count():,} rows.")
print(f'Max date: {df_weather_new.select([max("DATE")]).collect()[0]["max(DATE)"].strftime("%Y-%m-%d %H:%M:%S")}')

# COMMAND ----------

# MAGIC %md
# MAGIC Iteratively unpack the select statement used to find the maximum data 

# COMMAND ----------

#seems to find the row with the max date
df_weather_new.select([max("DATE")]).collect()

# COMMAND ----------

# adding the 0 just retrieves this Row from within that list
df_weather_new.select([max("DATE")]).collect()[0]

# COMMAND ----------

#max(Date) seems to have been assigned the datetime object and the ["max(DATE)"] object referenced it
df_weather_new.select([max("DATE")]).collect()[0]["max(DATE)"]

# COMMAND ----------

>>> df_weather_new.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Read in Airline Data (3m version)

# COMMAND ----------

# Load 2015 Q1 for Flights
df_airlines = spark.read.parquet("/mnt/mids-w261/datasets_final_project/parquet_airlines_data_3m/")
display(df_airlines)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rows and Columns and Airline Data

# COMMAND ----------

print(f"Your new df_airlines has {df_airlines.count():,} rows.")
print(f"Your new df_airlines has {len(df_airlines.columns)} rows.")


# COMMAND ----------

# MAGIC %md
# MAGIC Distinct Origin Airports

# COMMAND ----------

df_airlines.select("ORIGIN_AIRPORT_ID","ORIGIN_CITY_NAME" ).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Distinct Destintation Airports

# COMMAND ----------

df_airlines.select("DEST_AIRPORT_ID","DEST_CITY_NAME" ).distinct().count()

# COMMAND ----------

display(df_airlines)

# COMMAND ----------

# MAGIC %md 
# MAGIC Select Departure State where DAY_OF_WEEK is ODD

# COMMAND ----------

display(df_airlines.select("ORIGIN_STATE_ABR", df_airlines.DAY_OF_WEEK % 2 == 1 ))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC I only wanted to see the origin_state and day_of_week, not the logical condition. Reformulate the query to address. 

# COMMAND ----------

display(df_airlines[df_airlines.DAY_OF_WEEK % 2 == 1 ].select("DAY_OF_WEEK", "ORIGIN_STATE_ABR"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


