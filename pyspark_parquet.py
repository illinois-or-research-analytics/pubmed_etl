from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import time

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--tname", help="schema.table_name. Like hm31.george_pipeline")
parser.add_argument("--user", help="user of the db. Like hm31")
parser.add_argument("--pas", help="password of the db")
parser.add_argument("--path", help="path to the directory containing distributed parquet files")


args = parser.parse_args()
tname = args.tname
user = args.user
pas = args.pas
parquet_path = args.path



if parquet_path[-1] != '/':
    parquet_path += '/'




def insert_table(result, jdbc_url, jdbc_properties, tname):
    result.repartition(10).write.jdbc(url=jdbc_url, table=tname, mode="overwrite",properties=jdbc_properties)




spark = SparkSession.builder \
        .appName("parquet_unification") \
        .config("spark.executor.memory", "10g") \
        .getOrCreate()





df = spark.read.parquet(parquet_path)
spark.sparkContext.setLogLevel("WARN")

# df.printSchema()
start = time.time()

df = df.repartition(1)
df = df.dropDuplicates(['doi'])
init_count = df.count()
mid = time.time()

# print(df.count(),'asoon! drop duplicates', mid - start)
df.createOrReplaceTempView("temp_table")

sql_query = """
       WITH RankedRows AS (
           SELECT
               *,
               ROW_NUMBER() OVER (PARTITION BY PMID ORDER BY date_revised DESC) AS RowRank
           FROM
               temp_table
       )
       SELECT
           *
       FROM
           RankedRows
       WHERE
           RowRank = 1
"""

result_df = spark.sql(sql_query)
result_df = result_df.drop("RowRank")
result_df = result_df.filter(col("doi") != '')


after_query = time.time()
# print(f'run query in {after_query - mid}')s

george_df = result_df.withColumn("has_abstract", F.when(F.length("abstract") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_title", F.when(F.length("title") > 0, 1).otherwise(0))
george_df = george_df.withColumn("has_mesh", F.when(F.length("mesh") > 2, 1).otherwise(0))
george_df = george_df.withColumn("has_year", F.when(F.length("year") > 0, 1).otherwise(0))

george_df = george_df.select("PMID", "doi", "has_abstract", "has_title", "has_mesh", "has_year")
george_df = george_df.withColumnRenamed("PMID", "pmid")






jdbc_url = "jdbc:postgresql://valhalla.cs.illinois.edu:5432/ernieplus"

jdbc_properties = {
    "user": user,
    "password": pas,
    "driver": "org.postgresql.Driver"
}

insert_table(george_df, jdbc_url, jdbc_properties, tname)

end = time.time()
# print(f'Inserted into table: Elapsed time: {end - start} number of rows: {result_df.count()}')


spark.stop()



