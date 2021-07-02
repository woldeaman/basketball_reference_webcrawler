# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.contrib.exporter import CsvItemExporter


class BR_Data_Spider_Pipeline:
    def open_spider(self, spider):
        self.games_file = open(f'season_{self.season}_games.csv', 'wb')
        self.details_file = open(f'season_{self.season}_details.csv', 'wb')

    def close_spider(self, spider):
        self.games_file.close()
        self.details_file.close()
        # TODO: add chronological sorting for both files 

    def process_item(self, item, spider):
        # TODO
        if 'MP' in item.keys():
            # write to first csv file
        else:
            # write to other csv file
        return item