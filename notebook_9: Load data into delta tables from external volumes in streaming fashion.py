# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,DateType
file_schema = StructType([
    StructField("f_customer_id",IntegerType(), True),
    StructField("f_first_name", StringType(), True),
    StructField("f_last_name", StringType(), True),
    StructField("f_email", StringType(), True),
    StructField("f_phone_number", StringType(), True),
    StructField("f_date_of_birth", DateType(), True),
    StructField("f_joindate", DateType(), True),
    
])
raw_file_name='/Volumes/edsexternal/landingzone/raw/landingzone/crm/crm_data_1gb.csv'
file_df=spark.read.format("csv").options(header="true").schema(file_schema).load(raw_file_name)
table_nm=spark.read.table("edsmanaged.bronze.crm")
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
insert_records=join_df.filter(isnull(col("hash_val"))).drop(*table_column_names)
update_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")!=col("file_hash_val"))).drop(*table_column_names)
old_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")!=col("file_hash_val"))).drop(*file_df_cols)
similar_records=join_df.filter(isnotnull(col("hash_val")) & (col("hash_val")==col("file_hash_val"))).drop(*table_column_names)


display(join_df)
display(insert_records)
display(update_records)
display(old_records)
display(similar_records)

# COMMAND ----------

final_df = insert_records.union(update_records).union(similar_records)
final_df.write.insertInto("edsmanaged.bronze.crm",overwrite=True)
