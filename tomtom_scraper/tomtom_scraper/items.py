from scrapy.item import Item, Field


class TomtomTrafficItem(Item):
    city = Field()
    country = Field()
    update_time_live = Field()
    traffic_index_live = Field()
    jams_delay = Field()
    jams_length = Field()
    jams_count = Field()
    traffic_index_week_before = Field()
    update_time_week_before = Field()

