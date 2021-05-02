# scr-dw-af-dc_prod
## Preparation : choosing the right scraping tool
Understanding the structure of the source is important to decide either to use BS4 or scrapy. Scrapy seems very robust and allow 
- modification of the content on the fly
- allow direct dumping the data to postgresdb via pipeline

Inline-style: 
![alt text](https://github.com/msyafiqakmal/scr-dw-af-dc_prod/blob/master/image/marketwatch.png "Bursa Market Watch")


## Database setup
for both scenario im using postgres database which hosted in the same db. However, one can configured it to reside in multiple db via [Json config file](dags/mycompscraper/mytaskconfig.json):

`
{
    "scraptargethost"      : "192.168.0.112",
    "scraptargetdatabase"  : "BursaCompanies",
    "scraptargetuser"      : "user",
    "scraptargetpassword"  : "pwd",
    "dwoutputhost"      : "192.168.0.112",
    "dwoutputdatabase"  : "BursaCompanies",
    "dwoutputuser"      : "user",
    "dwoutputpassword"  : "pwd"
}
`

### DDL Script
There are 2 script developed each for the: 
- [dumping of scrapped data](dags/mycompscraper/DDL/source.sql)
- [target data warehouse table using star schema (Kimball methodology)](dags/mycompscraper/DDL/target.sql)
the creation of this tables requires an intervention beforehand

Inline-style: 
![alt text](https://github.com/msyafiqakmal/scr-dw-af-dc_prod/blob/master/image/datamodel.png "DDL")


## ETL Processing
Originally the plan is to use database engine to handle the creation and transformation. However, due to its complexity. Overall transformation is handled via pandas. Which means all data both in source and target need to be cleansed and merged before further transformed into the updated star schema records. 

In addition to this, instead of parsing the variable from one function to another. and due to my limited knowledge on xcom (i understand that it support movement of json data). I used csv to move data from one function to another. this resulted to the creation of the following 2 folders:
- [source](dags/mycompscraper/source/)
- [dimensional](dags/mycompscraper/source/)
to avoid further accumulation of garbage, the file will be cleansed after each iteration. 

ETL processing is devided into 6 main categories:
- the original scraping web to source database
- extract the scrapped source
- transform the data into dimension
- transform the fact table (this is due to dependency)
- trucate existing data
- load data into star schema

https://github.com/msyafiqakmal/scr-dw-af-dc_prod/blob/master/image/dag.png
Inline-style: 
![alt text](https://github.com/msyafiqakmal/scr-dw-af-dc_prod/blob/master/image/dag.png "DDL")

Actual output in db ide:
Inline-style: 
![alt text](https://github.com/msyafiqakmal/scr-dw-af-dc_prod/blob/master/image/ddlindbeaver.png "DDL in dbeaver")

Inline-style: 
![alt text](https://github.com/msyafiqakmal/scr-dw-af-dc_prod/blob/master/image/dbeaver.png"data in dbeaver")

I managed to implement error handling for some components. 
```
	try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print(table+" load complete")

````

## Airflow

I configuration is required is to define the absolute path in [dag file](dags/mycompscraper/scrapetodw.py)
````
currentpath="/home/msyafiqakmal/airflow/dags/mycompscraper"
````
this has dependencies on the database configuration and the csv creations.


## Docker Compose
For docker compose, I used airflow image prepared by puckle and I added [requirement file](dags/mycompscraper/requirements.txt) to ensure all python dependencies is addressed.
````
numpy==1.19.4
pandas==1.1.5
pytz==2020.1
Scrapy==2.5.0
requests==2.23.0
psycopg2==2.8.5
````
for execution simply invoke 
````
docker-compose up
````
in the root folder
