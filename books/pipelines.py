# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import hashlib


from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class MongoPipeline:
    COLLECTION_NAME = "books"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        #  creates a pipeline from a Crawler in order to make the general project settings available to the pipeline.
        return cls (
            mongo_uri= crawler.settings.get("MONGO_URI"),
            mongo_db= crawler.settings.get("MONGO_DATABASE"),
            )

    def process_item(self, item, spider):
        # gets called for every item pipeline component. It must either return an item or raise a DropItem exception.
        item_id = self.compute_item_id(item)
        item_dict = ItemAdapter(item).asdict()

        self.db[self.COLLECTION_NAME].update_one(\
            filter= {"_id": item_id},
            update = {"$set": item_dict},
            upsert= True
            )
        return item
    def compute_item_id(self, item):
        url = item["url"]
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def open_spider(self, spider):
        # gets called when the spider opens
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        # gets called when the spider closes
        self.client.close()
