import json
import sys
import wget
import xlrd
from xlrd import open_workbook
import requests
from lxml import etree

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

from jedeschule.spiders.bayern2 import Bayern2Spider
from jedeschule.spiders.bremen import BremenSpider
from jedeschule.spiders.brandenburg import BrandenburgSpider
from jedeschule.spiders.niedersachsen import NiedersachsenSpider
from jedeschule.spiders.nrw import NRWSpider
from jedeschule.spiders.sachsen import SachsenSpider
from jedeschule.spiders.sachsen_anhalt import SachsenAnhaltSpider
from jedeschule.spiders.thueringen import ThueringenSpider
from jedeschule.spiders.schleswig_holstein import SchleswigHolsteinSpider
from jedeschule.spiders.berlin import BerlinSpider


configure_logging()
settings = get_project_settings()
runner = CrawlerRunner(settings)

#url_mv = 'https://www.regierung-mv.de/serviceassistent/download?id=1599568'

def get_hamburg():
    url = 'https://geoportal-hamburg.de/geodienste_hamburg_de/HH_WFS_Schulen?REQUEST=GetFeature&SERVICE=WFS&SRSNAME=EPSG%3A25832&TYPENAME=staatliche_schulen&VERSION=1.1.0&outpuFormat=application/json'
    r = requests.get(url)
    r.encoding = 'utf-8'
    elem = etree.fromstring(r.content)
    data = []
    for member in elem:
        data_elem = {}
        for attr in member[0]:
            data_elem[attr.tag.split('}', 1)[1]] = attr.text
        data.append(data_elem)
    print('Parsed ' + str(len(data)) + ' data elements')
    with open('data/hamburg.json', 'w') as json_file:
        json_file.write(json.dumps(data))

def get_mv():
    url_mv = 'https://www.regierung-mv.de/serviceassistent/download?id=1614165'
    wget.download(url_mv, 'mv.xlsx')
    workbook = xlrd.open_workbook('mv.xlsx')
    data = []

    kuerzel = {}
    kuerzel['IGS'] = "Integrierte Gesamtschule"
    kuerzel['FöL/FöSp'] = 'Schule mit dem Förderschwerpunkt Lernen und dem Förderschwerpunkt Sprache'
    kuerzel['KGS/GS/\nFöL'] = 'Kooperative Gesamtschule mit Grundschule und Schule mit dem Förderschwerpunkt Lernen'
    kuerzel['FöG/FöKr'] = 'Schule mit dem Förderschwerpunkt Lernen und dem Förderschwerpunkt Unterricht kranker Schülerinnen und Schüler'

    # get all values from sheet "Legende"
    sheet = workbook.sheet_by_name("Legende")
    for row_num in range(sheet.nrows):
        row_value = sheet.row_values(row_num)
        if row_value[0] and row_value[1]:
            kuerzel[ row_value[0]] =  row_value[1]

    sheets = [
        'Schulverzeichnis öffentl. ABS', 
        'Schulverzeichnis öffentl. BLS',
        'Schulverzeichnis freie ABS']

    for sheet in sheets:
        worksheet = workbook.sheet_by_name(sheet)
        keys = [v.value for v in worksheet.row(0)]
        for row_number in range(worksheet.nrows):
            if row_number == 0:
                continue
            row_data = {}
            for col_number, cell in enumerate(worksheet.row(row_number)):
                row_data[keys[col_number]] = cell.value
            if row_data['Schulname']:
                
                # change key from Schul-behörde to Schulbehörde
                row_data['Schulbehörde'] = kuerzel[row_data['Schul-behörde']]
                del row_data['Schul-behörde']

                row_data['Landkreis/ kreisfr. Stadt'] = kuerzel[row_data['Landkreis/ kreisfr. Stadt']]

                if row_data.get('Schulart/ Org.form'):
                    row_data['Schulart/ Org.form'] = kuerzel[row_data['Schulart/ Org.form']]
                else:
                    row_data['Schulart/Org.form:'] = 'Berufliche Schule'

                # change key from Plz to PLZ
                row_data['PLZ'] = int(row_data['Plz'])
                del row_data['Plz']
                
                # add Schulstatus and separate from Schulname
                if '-Staatlich anerkannte Ersatzschule-' in row_data['Schulname']:
                       row_data['Schulstatus'] = 'Staatlich anerkannte Ersatzschule'
                else:
                    row_data['Schulname'] = row_data['Schulname'].split('-Staatlich')[0].strip()

                if '-Staatlich genehmigte Ersatzschule-' in row_data['Schulname']:
                       row_data['Schulstatus'] = 'Staatlich genehmigte Ersatzschule'
                else:
                    row_data['Schulname'] = row_data['Schulname'].split('-Staatlich')[0].strip()
                
                data.append(row_data)
                
                #print (40*'#')
                #for k, v in row_data.items():
                #    print(f"{k:20} {v}")

    with open('data/mecklenburg-vorpommern.json', 'w') as json_file:
        json_file.write(json.dumps(data))  
 


        
@defer.inlineCallbacks
def crawl():
    yield runner.crawl(BremenSpider)
    yield runner.crawl(BrandenburgSpider)
    yield runner.crawl(Bayern2Spider)
    yield runner.crawl(NiedersachsenSpider)
    yield runner.crawl(NRWSpider)
    yield runner.crawl(SachsenSpider)
    yield runner.crawl(SachsenAnhaltSpider)
    yield runner.crawl(ThueringenSpider)
    yield runner.crawl(SchleswigHolsteinSpider)
    yield runner.crawl(BerlinSpider)
    reactor.stop()

#crawl()
#reactor.run() # the script will block here until the last crawl call is finished
get_mv()
#get_hamburg()
