# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter


class BRDataSpiderPipeline:
    """Storing basic game data and detailed game data in different files."""
    SaveTypes = ['basic', 'detailed']

    def open_spider(self, spider):  # save basic and detailed data in separate files
        self.files = {id: open(f'season_{spider.season}_{id}.csv', 'wb') for id in self.SaveTypes}
        self.exporters = {id: CsvItemExporter(self.files[id]) for id in self.SaveTypes}
        for exp in self.exporters.values():  # start exporters up
            exp.start_exporting()

    def close_spider(self, spider):
        # TODO: add sorting step
        for id in self.SaveTypes:
            self.exporters[id].finish_exporting()
            self.files[id].close()
            
    def _item_type(self, item):
        return type(item).__name__.replace('GameData','').lower()  # BasicGameData -> basic

    def process_item(self, item, spider):
        # TODO: drop player data if they did not play
        item_id = self._item_type(item)
        assert item_id in self.SaveTypes  # make sure item_id is valid
        self.exporters[item_id].export_item(item)  # assign file according to item type
        # TODO: make export of nested "stats" dict work...
        return item