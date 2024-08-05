# Databricks notebook source
# MAGIC %sql
# MAGIC create table edsmanaged.retail.crm2(customer_id int ,first_name string,last_name string,phone_number string,email string,dob string,join_date string,hash_val string) USING DELTA
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history edsmanaged.retail.crm

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table edsmanaged.bronze.crm

# COMMAND ----------

# MAGIC %sql
# MAGIC create table edsmanaged.bronze.ecommerce(order_id int ,order_date string,customer_id string,product_id string,quantity string,price_per_unit string,total_amount string,review_rating string,review_text string) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC create table edsmanaged.bronze.point_of_sale(transaction_id string ,transaction_date string,store_id string,product_id string,quantity string,price_per_unit string,total_amount string) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC create table edsmanaged.bronze.social_media(post_id string ,post_timestamp string,platform string,content string,sentiment_score string,user_id string,post_likes string, post_shares string,post_comments string) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC create table edsmanaged.bronze.supply_chain(supplier_id string ,product_id string,inventory_level string,last_shipment_date string,next_shipment_date string) USING DELTA

# COMMAND ----------


