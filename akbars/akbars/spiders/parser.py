import json
import time

import scrapy


class ParserSpider(scrapy.Spider):
    name = "parser"
    url = "https://bankrot.fedresurs.ru"
    custom_settings = {
        "FEED_EXPORT_ENCODING": "utf-8",
    }
    json_file = "./data.json"

    def start_requests(self):
        try:
            with open(self.json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(e)
            return
        for el in data:
            fio = el["fio"]
            bd = el["bd"]
            url = f"{self.url}/backend/prsnbankrupts?searchString={fio}&isActiveLegalCase=null&limit=15&offset=0"
            yield scrapy.Request(url, callback=self.parse, meta={"birthday": bd, "fio": fio}, headers={"referer": f"{self.url}/bankrupts?searchString={fio}&isActiveLegalCase=null&limit=15&offset=0"})
            time.sleep(5)

    def parse(self, response):
        self.logger.info(f"Обрабатывается URL: {response.url}")
        data = json.loads(response.body)
        if data['total']:
            for el in data['pageData']:
                url = f"https://fedresurs.ru/backend/persons/{el['guid']}"
                meta = response.meta
                meta['guid'] = el['guid']
                yield scrapy.Request(url, callback=self.parse_birthday, meta=meta, headers={"referer": f"https://fedresurs.ru/persons/{el['guid']}"})

    def parse_birthday(self, response):
        self.logger.info(f"Обрабатывается информация по URL: {response.url}")
        data = json.loads(response.body)
        birthday = response.meta["birthday"]
        if data['birthdateBankruptcy'] == birthday:
            url = response.url + "/publications?limit=3&offset=0"
            yield scrapy.Request(url, callback=self.parse_details, meta=response.meta, headers={"referer": f"https://fedresurs.ru/persons/{response.meta['guid']}"})

    def parse_details(self, response):
        self.logger.info(f"Обрабатываются детали по URL: {response.url}")
        data = json.loads(response.body)
        if response.meta['fio'] == data['pageData'][0]['bankrupt']:
            yield {response.meta['fio']: data['pageData'][0].get("number", None)}
