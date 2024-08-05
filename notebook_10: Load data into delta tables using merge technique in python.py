# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Demo using the merge technique
# MAGIC OBSERVATION
# MAGIC  1. We have to write merge statement to perform insert , update and delete.
# MAGIC  2. Instead of how we are instructing what to do.
# MAGIC  3. Also if we look at the logs of this delta table we will find that everytime we perform insert , update and deletes the logs will tell us the exact count of insert update and deletes that was done during a transaction.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,DateType
file_schema = StructType([
    StructField("f_customer_id",IntegerType(), True),
    StructField("f_first_name", StringType(), True),
    StructField("f_last_name", StringType(), True),
    StructField("f_email", StringType(), True),
    StructField("f_phone_number", StringType(), True),
    StructField("f_date_of_birth", StringType(), True),
    StructField("f_joindate", StringType(), True),
    
])
raw_file_name='/Volumes/edsexternal/externallandingzone/unmanaged/retail-lob/crm/*'
file_df=spark.read.format("csv").options(header="true").schema(file_schema).load(raw_file_name)
table_nm=spark.read.table("edsmanaged.retail.crm2")
table_column_names = [c[0] for c in table_nm.dtypes]
display(file_df)





# COMMAND ----------

from pyspark.sql.functions import *

def trim_all_string_columns(df):
    return df.select(*[trim(col(c[0])).alias(c[0]) if c[1] == 'string' else col(c[0]) for c in df.dtypes])




all_columns=file_df.columns
file_df_new=file_df.transform(trim_all_string_columns)
add_hash_df=file_df.withColumn("file_hash_val", hash(concat_ws("||", *all_columns)) )
file_df_cols=[d[0] for d in add_hash_df.dtypes]
join_df=add_hash_df.join(table_nm,add_hash_df.f_customer_id == table_nm.customer_id,'fullouter')
active_insert_records=join_df.filter(isnull(col("hash_val"))).withColumn("flag",lit("I")).drop(*table_column_names)
active_update_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")!=col("file_hash_val"))).withColumn("flag",lit("U")).drop(*table_column_names)
expired_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")!=col("file_hash_val"))).withColumn("flag",lit("D")).drop(*file_df_cols)
delete_records=join_df.filter(isnull(col("file_hash_val"))).withColumn("flag",lit("D")).drop(*file_df_cols)
unchanged_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")==col("file_hash_val"))).withColumn("flag",lit("N")).drop(*table_column_names)

print("join_df_record")
display(join_df)
print("insert_record")
display(active_insert_records)
print("update_record")
display(active_update_records)
print("expired_records")
display(expired_records)
print("deleted_records")
display(delete_records)
print("unchanged_records")
display(unchanged_records)


# COMMAND ----------

from delta.tables import *
final_insert_df = active_insert_records.union(active_update_records).union(expired_records).union(delete_records)


delta_crm_df=DeltaTable.forName(spark,"edsmanaged.retail.crm2")
delta_crm_df.alias("target").merge(
    source=final_insert_df.alias("source"),
    condition="target.customer_id = source.f_customer_id").whenMatchedDelete(condition= "source.flag == 'D'").whenNotMatchedInsert( 
                         condition = "source.flag == 'I'",
                         values=
        {
        "customer_id":"f_customer_id",
        "first_name":"f_first_name",
        "last_name":"f_last_name",
         "email": "f_email",
         "phone_number":"f_phone_number",
         "dob":"f_date_of_birth",
          "join_date":"f_joindate",
          "hash_val": "file_hash_val"      
                          
        }
    ).whenMatchedUpdate( condition="source.flag== 'U'",set=
                      {
        "customer_id":"f_customer_id",
        "first_name":"f_first_name",
        "last_name":"f_last_name",
         "email": "f_email",
         "phone_number":"f_phone_number",
         "dob":"f_date_of_birth",
          "join_date":"f_joindate",
          "hash_val": "file_hash_val" 
                      }   
  ).execute()  

                                       


# COMMAND ----------

delta_crm_df.alias("target").merge(
source=final_delete_df.alias("source"),
condition="target.customer_id = source.customer_id").execute()      

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from edsmanaged.retail.crm2

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history edsmanaged.retail.crm2

# COMMAND ----------

# Assuming you have a DataFrame named df
final_insert_df.union(final_delete_df).createOrReplaceTempView("crm_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from crm_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO edsmanaged.retail.crm2 AS target
# MAGIC USING crm_view AS source
# MAGIC  ON target.customer_id = source.f_customer_id
# MAGIC WHEN MATCHED BY AND source.flag = 'U' THEN
# MAGIC   UPDATE SET
# MAGIC     customer_id = source.f_customer_id,
# MAGIC     first_name = source.f_first_name,
# MAGIC     last_name = source.f_last_name,
# MAGIC     email = source.f_email,
# MAGIC     phone_number = source.f_phone_number,
# MAGIC     dob = source.f_date_of_birth,
# MAGIC     join_date = source.f_joindate,
# MAGIC     hash_val = source.file_hash_val
# MAGIC WHEN NOT MATCHED AND source.flag = 'I' THEN
# MAGIC   INSERT(
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     phone_number,
# MAGIC     dob,
# MAGIC     join_date,
# MAGIC     hash_val
# MAGIC   ) VALUES (
# MAGIC     source.f_customer_id,
# MAGIC     source.f_first_name,
# MAGIC     source.f_last_name,
# MAGIC     source.f_email,
# MAGIC     source.f_phone_number,
# MAGIC     source.f_date_of_birth,
# MAGIC     source.f_joindate,
# MAGIC     source.file_hash_val
# MAGIC   )
# MAGIC --WHEN MATCHED AND source.flag = 'D' THEN
# MAGIC  -- DELETE
# MAGIC  -- We cannot use 

# COMMAND ----------


