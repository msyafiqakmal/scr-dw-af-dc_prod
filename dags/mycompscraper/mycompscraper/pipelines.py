# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
from datetime import datetime
import json

class MycompscraperPipeline(object):
    # def process_item(self, item, spider):
        # print("pipelineX: "+item['CompanyFullName'])
        # return item
    def open_spider(self, spider):
            with open('../mytaskconfig.json') as json_file:
                config = json.load(json_file)  
            hostname = config["scraptargethost"]
            username = config["scraptargetuser"]
            password = config["scraptargetpassword"]
            database = config["scraptargetdatabase"]
            self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        self.cur.execute("insert into scrapcomp(CompanyFullName,StockShortName,StockCode,MarketName,Shariah,Sector,MarketCap,LastPrice,PriceEarningRatio,DividentYield,ReturnOnEquity,LoadDateTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(item['CompanyFullName'],	item['StockShortName'],	item['StockCode'],	item['MarketName'],	item['Shariah'],	item['Sector'],	item['MarketCap'],	item['LastPrice'],	item['PriceEarningRatio'],	item['DividentYield'],	item['ReturnOnEquity'],datetime.now()))
        self.connection.commit()
        return item