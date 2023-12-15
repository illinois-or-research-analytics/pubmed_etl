#!/bin/bash


source ./venv/bin/activate



xml_input='/shared/hossein_hm31/xml_data'
parquet_output='/shared/hossein_hm31/pubmed_parquet'
cores=70
table_name='public.pubmed_etl'
user='hm31'
pas='graphs'

start_time=$(date +"%s")

python parallel.py -xml "$xml_input" -parquet "$parquet_output" -cores "$cores" -wrap 0
python parallel.py -xml "$xml_input" -parquet "$parquet_output" -cores "$cores" -wrap 1


mid_time=$(date +"%s")
elapsed_time_parse=$((mid_time - start_time))



spark-submit --master local[*] --jars './postgresql-42.5.2.jar' --driver-memory 220g  --conf "spark.local.dir=./logs" pyspark_parquet.py --tname "$table_name" --user "$user" --pas "$pas" --path "$parquet_output"

end_time=$(date +"%s")

elapsed_time_data=$((end_time - mid_time))


echo "Elapsed time for parsing: $elapsed_time_parse seconds"
echo "Elapsed time for data handling: $elapsed_time_data seconds"
