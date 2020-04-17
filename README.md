#COVID19 Analysis

**Tracks COVID19 Cases**

This project consolidates two of the major COVID19 data repositories and consolidates and standardizes two disparate sources.

Dependencies:
* [Corona Data Scraper](https://coronadatascraper.com/#home)
* [Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19)
* [AWS S3](https://aws.amazon.com/s3/)
* [Apache Spark 2.4.5](https://spark.apache.org/)
* [COVID19 ELT Pipeline Repo](https://github.com/polyglotDataNerd/poly-covid19)
* [Scala 2.12](https://www.scala-lang.org/download/2.12.10.html)

Intention
-
* The intention of this repo is to understand and analyze the consolidated data using Spark Dataframes. 

Frequency
-  
* Dependant on the frequency of the extract and load pipeline, all data will be sourced in s3 via the objects that is extracted by the data pipeline. 

Output
-
* Refer to [Readme.md](/output)
* Made s3 prefix public to download individual sources files along with combined sources
    - All objects are compressed in GZIP format

            -Johns Hopkins
            aws s3 ls s3://poly-testing/covid/jhu  --recursive
            2020-04-01 08:43:11          0 covid/jhu/
            2020-04-02 05:30:58     329761 covid/jhu/UID_ISO_FIPS_LookUp_Table.csv
            2020-04-01 08:43:19          0 covid/jhu/raw/
            2020-04-17 05:13:43     314337 covid/jhu/raw/04-16-2020.csv
            2020-04-17 05:14:48    1223240 covid/jhu/transformed/2020-04-17/jhu_2020-04-17.gz
            
            -Data Scraper
            aws s3 ls s3://poly-testing/covid/cds  --recursive
            2020-04-17 05:14:49     819222 covid/cds/2020-04-17/cds_2020-04-17.gz
            
            -Combined
            aws s3 ls s3://poly-testing/covid/combined  --recursive
            2020-04-17 05:25:51          0 covid/combined/_SUCCESS
            2020-04-17 05:25:49    3834451 covid/combined/part-00000-956ddeaf-0cac-472a-9098-0d86362a9e52-c000.csv.gz

        