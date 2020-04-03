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
        