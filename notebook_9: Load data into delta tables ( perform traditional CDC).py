# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Demo using the old method where we join two dfferent datasets
# MAGIC OBSERVATION
# MAGIC  1. We have to write logic ti implement the concept of insert , update and delete
# MAGIC  2. We have to tell spark How to do this insert update and delete
# MAGIC  3. Also if we look at the logs of this delta table we will find that everytime we perform insert , update and deletes the logs dont give us inforelated to these operations . Inspite of that it gives the count of the no of record written .
# MAGIC  4. The logs using the traditional method can not help us in understanding the transactions that occurrred on this table with respect to insert update and deletes.
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
table_nm=spark.read.table("edsmanaged.retail.crm")
table_column_names = [c[0] for c in table_nm.dtypes]
file_df_cols=[d[0] for d in file_df.dtypes]
display(file_df)



# COMMAND ----------

from pyspark.sql.functions import *

def trim_all_string_columns(df):
    return df.select(*[trim(col(c[0])).alias(c[0]) if c[1] == 'string' else col(c[0]) for c in df.dtypes])




all_columns=file_df.columns
file_df_new=file_df.transform(trim_all_string_columns)
add_hash_df=file_df.withColumn("file_hash_val", hash(concat_ws("||", *all_columns)) )
join_df=add_hash_df.join(table_nm,add_hash_df.f_customer_id == table_nm.customer_id,'fullouter')
active_insert_records=join_df.filter(isnull(col("hash_val"))).drop(*table_column_names)
active_update_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")!=col("file_hash_val"))).drop(*table_column_names)
expired_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")!=col("file_hash_val"))).drop(*file_df_cols)
unchanged_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")==col("file_hash_val"))).drop(*table_column_names)

print("jpin_df_record")
display(join_df)
print("insert_record")
display(active_insert_records)
print("update_record")
display(active_update_records)
print("expired_records")
display(expired_records)
print("unchanged_records")
display(unchanged_records)

# COMMAND ----------



# COMMAND ----------

print(active_insert_records.count())

# COMMAND ----------

final_df = active_insert_records.union(active_update_records).union(unchanged_records)
if active_insert_records.count() != 0 or active_update_records.count() !=0 :
    final_df.write.insertInto("edsmanaged.retail.crm",overwrite=True)
    print("retail.crm table load successful")
else:
    print("No new record found for update or insert")




# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history edsmanaged.retail.crm
