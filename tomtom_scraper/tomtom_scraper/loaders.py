from itemloaders.processors import TakeFirst
from scrapy.loader import ItemLoader

class ItemLoader(ItemLoader):
    default_output_processor = TakeFirst()