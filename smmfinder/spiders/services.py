from scrapy.spiders import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
import scrapy
from scrapy.http import FormRequest
from scrapy.crawler import CrawlerProcess
import sys
import re
import os
import pandas
from forex_python.converter import CurrencyRates
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import sites, currency_dict, services

class Services(Spider):
    name = 'services'
    start_urls = sites

    '''
    def currency_converter(self, price):
        c = CurrencyRates()
        currency_symbol = re.findall(r'[A-Z$€£¥₹₽₦₺]+', price)
        number = float(re.findall('\d+', price)[0])
        if len(currency_symbol) > 0:
            currency_symbol = currency_symbol[0]
            # Get currency code from dictionary
            currency_code = currency_dict.get(currency_symbol)
            return c.convert(currency_code, 'USD', number)
        else:
            raise Exception
    '''
    def currency_converter(self, price):
        c = CurrencyRates()
        currency_symbol = re.findall(r'[A-Z$€£¥₹₽₦₺]+', price)
        number = float(re.findall('[\d.]+', price)[0])
        if len(currency_symbol) > 0:
            currency_symbol = currency_symbol[0]
            if currency_symbol == '$':
                currency_code = 'USD'
            else:
                # Get currency code from dictionary
                currency_code = currency_dict.get(currency_symbol)
            return c.convert(currency_code, 'USD', number)
        else:
            raise Exception
    
    def convert_to_minutes(self, time_string):
        time = time_string.split()
        if len(time) == 4:
            hours = int(time[0]) * 60
            minutes = int(time[2])
            total = hours + minutes
        elif len(time) == 6:
            days = int(time[0]) * 24 * 60
            hours = int(time[2]) * 60
            minutes = int(time[4])
            total = hours + days + minutes
        elif len(time) < 4:
            total = int(time[0])
        return total

    def parse(self, response):
        rows = response.xpath("//tr[contains(@data-filter-table-category-id, '')][not(contains(@class, 'visible-xs visible-sm service-description'))]")
        #print("Rows: ", len(rows))
        for index, service in services.iterrows():
            try:
                id = int(service['ID'])
                sName = service['ServiceName']
                minimum = int(service['Min Order'])
                maximum = int(service['Max Order'])
                maxPrice = float(service['Supplier'].replace('$', ''))
            except Exception as e:
                print(e)
            for row in rows:
                i = 0
                if len(row.xpath(".//td")) >= 5:
                    #name
                    try:
                        name = row.xpath(".//td[2]/text()").get().strip()
                        first_word = sName.split()[0]
                        if " ".join(sName.split()[0:2]) not in name:
                            #print(" ".join(sName.split()[0:2]))
                            raise Exception
                        elif fuzz.partial_ratio(name, sName) < 40:
                            raise Exception
                    except:
                        #print("name")
                        continue

                    #Min
                    try:
                        min = int(row.xpath(".//td[4]/text()").extract_first().strip())
                        if min != minimum:
                            raise Exception
                    except:
                        #print("min")
                        continue

                    #Max
                    try:
                        max = int(row.xpath(".//td[5]/text()").extract_first().strip())
                        if max != maximum:
                            raise Exception
                    except:
                        #print("max")
                        continue

                    #price
                    try:
                        price = row.xpath(".//td[3]//text()").get().strip()
                        price = float(self.currency_converter(price))
                        if price > maxPrice:
                            raise Exception
                        price_diff_percent = price / maxPrice * 100 
                    except Exception as e:
                        #print(e)
                        continue

                    #Average Time
                    try:
                        time = row.xpath(".//td[6]/text()").get().strip()
                        time = self.convert_to_minutes(time)
                    except:
                        time = ''
                        pass

                    #Desc: .//div[contains(@id, 'service-description') or contains(@class, 'modal-body')]
                    try:
                        desc = row.xpath(".//*[contains(@id, 'service-description') or contains(@class, 'modal-body') or contains(@class, 'visible-xs visible-sm service-description')]/descendant-or-self::text()").getall()
                        desc = " ".join(desc)
                        #print(desc)
                        if desc is None:
                            raise Exception
                    except:
                        desc = ''
                        pass
                    
                    data = {
                        'name': name,
                        'price(In USD)': price,
                        'min': min,
                        'max': max,
                        'description': desc,
                        'Time(In Minutes)': time,
                        'Price Difference (%)': price_diff_percent,
                        'panel': response.url,
                        'Your Service': id
                    }

                    yield data


def run():
    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(Services)
    process.start()


if __name__ == '__main__':
    run()
