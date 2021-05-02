from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import pandas.io.sql as sqlio
import os

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'M Syafiq Akmal',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('Scraper_to_DataWarehouse', default_args=default_args)

#initialization for Scrapy
scrapy_path="/home/msyafiqakmal/airflow/dags/mycompscraper/mycompscraper"
currentpath="/home/msyafiqakmal/airflow/dags/mycompscraper"

#initialization for DW onwards
scr_db_conn = {
    "host"      : "192.168.0.112",
    "database"  : "BursaCompanies",
    "user"      : "msyafiqakmal",
    "password"  : "M19yosil"
}

dw_db_conn = {
    "host"      : "192.168.0.112",
    "database"  : "DW_MarketWatch",
    "user"      : "msyafiqakmal",
    "password"  : "M19yosil"
}

#Utility Functions
def connect(conn_info):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**conn_info)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


connSrc = connect(scr_db_conn)
connDW = connect(dw_db_conn)
latestdate = pd.DataFrame()
latestmw = pd.DataFrame()
DW_tables = ["fact_marketwatch","dim_bursacomp","dim_date","dim_market","dim_sector","dim_shariahcompliance"]
DW_pd = {}

#Utility Functions
def execute_values(conn, df, table):

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print(table+" load complete")
    cursor.close()

# ETL Functions
def extract_Source():
    #get latest Source data
    sqlSrc = '''select sc.* from scrapcomp sc 
    left join 
    (select max(loaddatetime)as max_date from scrapcomp)b
    on date(sc.loaddatetime) = date(b.max_date)'''
    sqlio.read_sql_query(sqlSrc, connSrc).to_csv(currentpath+"/source/latestmw.csv")
    connSrc.commit()

    #get latest DW data and initialization
    fact_query = '''select 
    db.companyfullname,
    db.stockshortname,
    db.stockcode,
    dm.marketname,
    dsc.shariah,
    ds.sector,
    fm.marketcap,
    fm.lastprice,
    priceearningratio,
    dividentyield ,
    returnonequity,
    dd.loaddate 
    from fact_marketwatch fm 
    left join dim_bursacomp db on db.company_key = fm.company_key 
    left join dim_date dd on dd.date_key =fm.loaddate_key 
    left join dim_market dm on dm.market_key = fm.market_key 
    left join dim_sector ds on ds.sector_key = fm.sector_key 
    left join dim_shariahcompliance dsc on dsc.shariah_key = fm.shariah_key  '''

    for table in DW_tables:
        if(table == "fact_marketwatch"):
            sqlio.read_sql_query(fact_query, connDW).to_csv(currentpath+"/source/"+table+".csv")
            print(table)
        else:
            sqlio.read_sql_query("select * from "+table, connDW).to_csv(currentpath+"/source/"+table+".csv")
            print(table)
    connDW.commit()



def dimension_transformation():
    
    for table in DW_tables:
        DW_pd[table]= pd.read_csv(currentpath+"/source/"+table+'.csv')
        DW_pd[table].drop(DW_pd[table].columns[0],axis=1,inplace=True)
    
    latestmw = pd.read_csv(currentpath+"/source/latestmw.csv")
    latestmw.drop(latestmw.columns[0],axis=1,inplace=True)
    
    DW_pd["dim_shariahcompliance"] = pd.concat([DW_pd['dim_shariahcompliance'][['shariah']],latestmw[['shariah']]])
    DW_pd["dim_shariahcompliance"]= DW_pd["dim_shariahcompliance"].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"shariah_key"})

    DW_pd["dim_sector"] = pd.concat([DW_pd['dim_sector'][['sector']],latestmw[['sector']]])
    DW_pd["dim_sector"]= DW_pd["dim_sector"].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"sector_key"})

    DW_pd["dim_market"] = pd.concat([DW_pd['dim_market'][['marketname']],latestmw[['marketname']]])
    DW_pd["dim_market"] = DW_pd["dim_market"].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"market_key"})

    DW_pd["dim_bursacomp"]=pd.concat([DW_pd["dim_bursacomp"][['companyfullname','stockshortname','stockcode']],latestmw[['companyfullname','stockshortname','stockcode']]])
    DW_pd["dim_bursacomp"]=DW_pd["dim_bursacomp"].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"company_key"})

    latestmw["loaddate"]=latestmw["loaddatetime"].apply(lambda x :datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').date() )

    latestdate["loaddate"] = latestmw["loaddate"].drop_duplicates()
    latestdate["day"] = latestdate["loaddate"].apply(lambda x :pd.to_datetime(x).day )
    latestdate["dayofweek"] = latestdate["loaddate"].apply(lambda x :pd.to_datetime(x).day_name() )
    latestdate["month"] = latestdate["loaddate"].apply(lambda x :pd.to_datetime(x).month_name() )
    latestdate["quarter"] = latestdate["loaddate"].apply(lambda x :pd.to_datetime(x).quarter )
    latestdate["year"] = latestdate["loaddate"].apply(lambda x :pd.to_datetime(x).year )

    DW_pd["dim_date"]=pd.concat([DW_pd["dim_date"][['loaddate','day','dayofweek','month','quarter','year']],latestdate[['loaddate','day','dayofweek','month','quarter','year']]])
    DW_pd["dim_date"]=DW_pd["dim_date"].dropna().drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"date_key"})

    # save and cleanup
    for table in DW_tables:
        DW_pd[table].to_csv(currentpath+"/dimensional/"+table+'.csv')

        if os.path.exists(currentpath+"/source/"+table+'.csv'):
            os.remove(currentpath+"/source/"+table+'.csv')

    latestmw.to_csv(currentpath+"/dimensional/latestmw.csv")
    if os.path.exists(currentpath+"/source/latestmw.csv"):
        os.remove(currentpath+"/source/latestmw.csv")
    

def fact_transformation():
    latestmw = pd.read_csv(currentpath+"/dimensional/latestmw.csv")
    latestmw.drop(latestmw.columns[0],axis=1,inplace=True)
    for table in DW_tables:
        DW_pd[table]= pd.read_csv(currentpath+"/dimensional/"+table+'.csv')
        DW_pd[table].drop(DW_pd[table].columns[0],axis=1,inplace=True)

    #cleanup source datatype
    latestmw["lastprice"] = pd.to_numeric(latestmw["lastprice"], errors='coerce')
    latestmw["priceearningratio"] = pd.to_numeric(latestmw["priceearningratio"], errors='coerce')
    latestmw["dividentyield"] = pd.to_numeric(latestmw["dividentyield"], errors='coerce')
    latestmw["returnonequity"] = pd.to_numeric(latestmw["returnonequity"], errors='coerce')
        
    #merge source and current DW
    DW_pd["fact_marketwatch"]=pd.concat([DW_pd["fact_marketwatch"][['companyfullname','stockshortname','stockcode','marketname','shariah','sector','marketcap','lastprice','priceearningratio','dividentyield','returnonequity','loaddate']],latestmw[['companyfullname','stockshortname','stockcode','marketname','shariah','sector','marketcap','lastprice','priceearningratio','dividentyield','returnonequity','loaddate']]])
    DW_pd["fact_marketwatch"]=DW_pd["fact_marketwatch"].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"id"})
   
    DW_pd["fact_marketwatch"] = pd.merge(DW_pd["fact_marketwatch"],DW_pd["dim_bursacomp"][["company_key","stockcode"]],left_on="stockcode",right_on="stockcode",how='left')
    DW_pd["fact_marketwatch"] = pd.merge(DW_pd["fact_marketwatch"],DW_pd["dim_date"][["date_key","loaddate"]],left_on="loaddate",right_on="loaddate",how='left')
    DW_pd["fact_marketwatch"] = pd.merge(DW_pd["fact_marketwatch"],DW_pd["dim_market"][["market_key","marketname"]],left_on="marketname",right_on="marketname",how='left')
    DW_pd["fact_marketwatch"] = pd.merge(DW_pd["fact_marketwatch"],DW_pd["dim_sector"][["sector_key","sector"]],left_on="sector",right_on="sector",how='left')
    DW_pd["fact_marketwatch"] = pd.merge(DW_pd["fact_marketwatch"],DW_pd["dim_shariahcompliance"][["shariah_key","shariah"]],left_on="shariah",right_on="shariah",how='left')
    DW_pd["fact_marketwatch"]["loaddate_key"] = DW_pd["fact_marketwatch"]["date_key"] 
    
    DW_pd["fact_marketwatch"]=DW_pd["fact_marketwatch"][["id","company_key","market_key","shariah_key","sector_key","marketcap","lastprice","priceearningratio","dividentyield","returnonequity","loaddate_key"]] 
    
    DW_pd["fact_marketwatch"].to_csv(currentpath+"/dimensional/fact_marketwatch.csv")
    # cleanup
    if os.path.exists(currentpath+"/dimensional/latestmw.csv"):
        os.remove(currentpath+"/dimensional/latestmw.csv")

def dw_truncate():
    global DW_tables
    for table in list(reversed(DW_tables)):
        curr = connDW.cursor()
        curr.execute('truncate '+table+' cascade')

def dw_load():
    for table in list(reversed(DW_tables)):
        DW_pd[table]= pd.read_csv(currentpath+"/dimensional/"+table+'.csv')
        DW_pd[table].drop(DW_pd[table].columns[0],axis=1,inplace=True)
        execute_values(connDW,DW_pd[table],table)
        if os.path.exists(currentpath+"/dimensional/"+table+'.csv'):
            os.remove(currentpath+"/dimensional/"+table+'.csv')


t1 = BashOperator(
    task_id='scrape_web_to_sourcedb',
    bash_command='cd {} && scrapy crawl CompanyScraper'.format(scrapy_path),
    dag=dag)

t2 = PythonOperator(
    task_id='extract_source_db',
    python_callable=extract_Source,
    dag=dag)

t3 = PythonOperator(
    task_id='transform_dimension',
    python_callable=dimension_transformation,
    dag=dag)

t4 = PythonOperator(
    task_id='transform_fact',
    python_callable=fact_transformation,
    dag=dag)

t5 = PythonOperator(
    task_id='trucate_existing_dw_content',
    python_callable=dw_truncate,
    dag=dag)

t6 = PythonOperator(
    task_id='load_star_schema',
    python_callable=dw_load,
    dag=dag)


t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)