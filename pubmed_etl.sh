#!/bin/bash


source ./venv/bin/activate



gz_input='/shared/pcopy'
parquet_output='/shared/hossein_hm31/parquet_test'
cores=70
table_name='hm31.pubmed_etl_performance_test'
user='hm31'
pas='graphs'

start_time=$(date +"%s")


python extractor.py -gz "$gz_input" -cores "$cores"


mid_time1=$(date +"%s")
elapsed_time_unzip=$((mid_time1 - start_time))

python parallel.py -xml "$gz_input" -parquet "$parquet_output" -cores "$cores" -wrap 0
python parallel.py -xml "$gz_input" -parquet "$parquet_output" -cores "$cores" -wrap 1


mid_time2=$(date +"%s")
elapsed_time_parse=$((mid_time2 - mid_time1))



spark-submit --master local[*] --jars './postgresql-42.5.2.jar' --driver-memory 220g  --conf "spark.local.dir=./logs" pyspark_parquet.py --tname "$table_name" --user "$user" --pas "$pas" --path "$parquet_output"

end_time=$(date +"%s")

elapsed_time_data=$((end_time - mid_time))

echo "Elapsed time for unzipping: elapsed_time_unzip seconds"
echo "Elapsed time for parsing: $elapsed_time_parse seconds"
echo "Elapsed time for data handling: $elapsed_time_data seconds"
