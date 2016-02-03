# Spark Job to create Parquet files from text file

This Job will keep (or not) partitions in HIVE 

Let's say we have this a HDFS - HIVE table partitioned like this. All the data is stored like text (CSV):
  root
   |--year
   |--month
   |--day
   |--hour
 
 And we want to create:
 1. A parquet file containing all the data OR 
 2. Exactly the same partitioning but with Parquet files instead text (CSV) files.
 
For writting the data, we merge schema to have schema evolution:
[merge schema](http://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging)


## Submit

```
spark-submit \
--class CSV2Parquet.Csv2Parquet \  
--master spark://10.0.0.1:7077 \
--executor-memory 10g \
--driver-memory 10g \
--packages com.databricks:spark-csv_2.11:1.3.0 \
csv2parquet_2.11-1.0.jar
``