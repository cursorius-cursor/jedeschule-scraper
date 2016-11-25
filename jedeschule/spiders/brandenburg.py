# -*- coding: utf-8 -*-
import scrapy
from scrapy.shell import inspect_response


class BrandenburgSpider(scrapy.Spider):
    name = "brandenburg"
    # allowed_domains = ["https://www.bildung-brandenburg.de/schulportraets/index.php?id=3"]
    start_urls = ['https://www.bildung-brandenburg.de/schulportraets/index.php?id=3&schuljahr=2016&kreis=&plz=&schulform=&jahrgangsstufe=0&traeger=0&submit=Suchen',
                  'https://www.bildung-brandenburg.de/schulportraets/index.php?id=3&schuljahr=2016&kreis=&plz=&schulform=&jahrgangsstufe=0&traeger=1&submit=Suchen']

    def parse(self, response):
        for link in response.css("table a ::attr(href)").extract():
            yield scrapy.Request(response.urljoin(link), callback=self.parse_detail)

    def parse_detail(self, response):
        trs = response.css("table tr")
        content = {}
        # The first row is an image and a map
        for tr in trs[1:]:
            key = "\n".join(tr.css('th ::text').extract())
            value = "\n".join(tr.css("td ::text").extract())
            content[key] = value
        yield content
