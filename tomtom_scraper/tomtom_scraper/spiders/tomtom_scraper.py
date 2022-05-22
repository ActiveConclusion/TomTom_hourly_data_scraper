import imp
import jmespath
from scrapy import Spider, Request
from tomtom_scraper.loaders import ItemLoader
import pandas as pd
from pathlib import Path
from tomtom_scraper.items import TomtomTrafficItem


class TomTomSpider(Spider):
    name = "tomtom_scraper"
    allowed_domains = ["tomtom.com"]

    PAGE_DATA_URL = (
        "https://www.tomtom.com/en_gb/traffic-index"
        "/page-data/ranking/page-data.json"
    )
    CITY_KEY_MATCH_FILE = Path('auxiliary_data/city_key_match.csv')
    ALPHA_CODES_FILE = Path('auxiliary_data/country_alpha_codes.csv')
    BASE_API_URL = 'https://api.midway.tomtom.com/ranking/liveHourly/'

    def start_requests(self):
        yield Request(url=self.PAGE_DATA_URL, callback=self.parse)

    def parse(self, response):
        page_data = response.json()
        city_data = self._parse_city_data(page_data)
        key_match_df = pd.read_csv(self.CITY_KEY_MATCH_FILE)
        key_match_dict = dict(key_match_df.values)
        city_data.replace({"key": key_match_dict}, inplace=True)
        alpha_codes = pd.read_csv(self.ALPHA_CODES_FILE)
        city_data = pd.merge(
            city_data, 
            alpha_codes, 
            left_on="country_code", 
            right_on="Alpha2", 
            how="left",
        )
        city_data['api_key'] = city_data["Alpha3"] + "_" + city_data["key"]
        city_df_list = []
        for _, row in city_data.iterrows():
            api_url = ''.join((self.BASE_API_URL, row["api_key"]))
            yield Request(
                url=api_url, 
                callback=self.parse_city_traffic,
                meta={'row': row},
            )


        print(key_match_dict)
        print(city_data.head())
    
    def parse_city_traffic(self, response):
        full_json = response.json()
        if response.status not in [200] or not full_json:
            return
        data = jmespath.search('data[]', full_json) or []
        row = response.meta.get('row')
        for hourly_data in data:
            l = ItemLoader(item=TomtomTrafficItem(), response=response)
            l.add_value('city', row.get('city'))
            l.add_value('country', row.get('country'))
            l.add_value('update_time_live', jmespath.search('UpdateTime', hourly_data))
            l.add_value('traffic_index_live', jmespath.search('TrafficIndexLive', hourly_data))
            l.add_value('jams_delay', jmespath.search('JamsDelay', hourly_data))
            l.add_value('jams_length', jmespath.search('JamsLength', hourly_data))
            l.add_value('jams_count', jmespath.search('JamsCount', hourly_data))
            l.add_value('traffic_index_week_before', jmespath.search('TrafficIndexWeekAgo', hourly_data))
            l.add_value('update_time_week_before', jmespath.search('UpdateTimeWeekAgo', hourly_data))
            yield l.load_item()

    def _parse_city_data(self, page_data):
        j = (
            "result.data.allCitiesJson.edges[].node"
            ".[name, key, country, countryName]"
        )
        data = jmespath.search(j, page_data)
        columns = ['city', 'key', 'country_code', 'country']
        return pd.DataFrame(data, columns=columns)
