# Pbumed ETL pipeline


A pipeline to parse XML files of PubMed into distributed parquet files, aggregate these records, and generate a table indicating pmid, doi of papers, and the presence or absence of these features on the metadata:


- Year of journal publication
- MeSH
- Title
- Abstract


For duplicate pmids, we only take the latest record mentioned by the latest revise date. We also drop all duplicate dois after we convert them to lowercase. We also discard all records with no doi.


## Installation

Install the requirements.txt into a Venv directory along with the downloaded files. We used Python 3.6.8.

## Usage

- change gz_input to indicate the directory where all gz files exist. There should be no extra file in that directory. Keey a copy of that directory since all gz files will be extracted.
- change parquet_output to indicate an empty directory where distributed parquet files will be saved.
- cores is the number of cores. 70 Is great for Valhalla, But generally, divide your free ram by 4 and set that as input.
- user is the username to the DB
- pas is the password to the DB
- The driver memory and executor memory are configured for Valhalla and may require further fine-tuning
- Run the pipeline where no other significant job is running, or limit the resources (cores, driver memory, executor memory)
- The pipeline should take around 1 hour to complete
- As a cross-check, it is worth to the check the number of parquet files is equal to the number of XML files


Running the pipeline:
```sh
 ./pubmed_etl.sh
```


## Results
Once the pipeline is finished, results will be saved in the table mentioned. 0 indicates a missing feature and 1 indicates an existing feature.

