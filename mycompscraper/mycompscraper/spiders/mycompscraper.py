import scrapy
import re
import string

class MyCompScraper(scrapy.Spider):
    name = 'CompanyScraper'

    allasciinzero = [char for char in string.ascii_uppercase]
    allasciinzero.insert(0,'0')
    for i in range(len(allasciinzero)):
        allasciinzero[i] = 'https://www.malaysiastock.biz/Listed-Companies.aspx?type=A&value='+allasciinzero[i]
    start_urls = allasciinzero

    def parse(self, response):
        for companies in response.xpath('//*[@id="companyInfo"]/table/tr'):
            try:
                splitStockNameCode = companies.css('a::text').get().split(' ',1)
                yield {
                    'CompanyFullName': companies.css('h3::text').get(),
                    'StockShortName':splitStockNameCode[0],
                    'StockCode':splitStockNameCode[1].strip("()"),
                    'MarketName':  companies.css('span::text').get(),
                    'Shariah': re.search('https://www.malaysiastock.biz/App_Themes/images/(.+?).png', companies.css('img::attr(src)').extract()[0]).group(1),
                    'Sector': companies.css('h3::text').extract()[1],
                    'MarketCap':companies.css('td::text').extract()[0],
                    'LastPrice':companies.css('td::text').extract()[1],
                    'PriceEarningRatio':companies.css('td::text').extract()[2],
                    'DividentYield':companies.css('td::text').extract()[3],
                    'ReturnOnEquity':companies.css('td::text').extract()[4]
                }
            
            except:
                    print('data doesnt comply with the requested structure')