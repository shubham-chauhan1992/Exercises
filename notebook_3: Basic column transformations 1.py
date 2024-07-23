# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise : 
# MAGIC
# MAGIC 1. print the schema by reading the file first using dataframe.schema  option.
# MAGIC 2. Copy the schema and change the column names with the correct naming convention and data type of the column names.
# MAGIC 3. read the file with this declared new schema.
# MAGIC 1. add a new column from existing columns in the dataframe
# MAGIC 2. Add multiple new columns in the dataframe.
# MAGIC 2. change the name of an existing column in the 
# MAGIC 3. Drop columns from a dataframe
# MAGIC 8. Select particular columns from a dataframe
# MAGIC

# COMMAND ----------

# Ans 1
df=spark.read.format('csv').options(header="true",inferSchema='true').load("/Volumes/eds_external_ingestion_source/landingzone/sources/landingzone/customer/")

display(df)

# COMMAND ----------

# Ans 1 continued
print(df.schema)

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,DateType,IntegerType,StructField
# Ans 2 
csv_file_schema=StructType([StructField('index', IntegerType(), True), StructField('customer_id', StringType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('company', StringType(), True), StructField('city', StringType(), True), StructField('country', StringType(), True), StructField('phone_1', StringType(), True), StructField('phone_2', StringType(), True), StructField('email', StringType(), True), StructField('subscription_date', DateType(), True), StructField('website', StringType(), True)])



# Ans 3
custom_schema_df=spark.read.format('csv').options(header="true").schema(csv_file_schema).load("/Volumes/eds_external_ingestion_source/landingzone/sources/landingzone/customer/*")
display(custom_schema_df)



# COMMAND ----------

from pyspark.sql.functions import *
# Ans 4.
addColumnDf=custom_schema_df.withColumn("email_domain_name",split(col("email"),"@")[1])
display(addColumnDf)
# Ans 5.
addMultipleColumnDf1=custom_schema_df.withColumn("email_domain_name",split(col("email"),"@")[1]).withColumn("Null_Flag",when(col("phone_1").isNaN() | col("phone_1").isNull(),"yes").otherwise("no") )
display(addMultipleColumnDf1)
# Ans 5 continued.
addMultipleColumnDf2=custom_schema_df.withColumns({
    "email_domain_name": split(col("email"),"@")[1],
    "Blank_flag": when(trim(col("phone_1"))=="","yes").otherwise("no") 
})
display(addMultipleColumnDf2)


# COMMAND ----------

from pyspark.sql.functions import lit
# Ans 6.
renameColumnDf=custom_schema_df.withColumnRenamed("customer_id","customer_ref").withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name") ))
display(renameColumnDf)



# COMMAND ----------

# Ans 7
dropColumnDf=custom_schema_df.withColumnRenamed("customer_id","customer_ref").withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name") )).drop("first_name","last_name")
display(dropColumnDf)

# COMMAND ----------

selectColumnDf=custom_schema_df.withColumnRenamed("customer_id","customer_ref").withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name") )).select("first_name","last_name")
display(selectColumnDf)

# COMMAND ----------


