"""Gather game and player data from basketball-reference.com"""
# -*- coding: utf-8 -*-
import scrapy
import datetime as dt

class GamesSpider(scrapy.Spider):
    season='2020'
    name = "nba_games"
    custom_settings = {'FEED_FORMAT': 'csv', 'FEED_URI': f'season_{season}_data.csv'}
    start_urls = [f'https://www.basketball-reference.com/leagues/NBA_{season}_games']
    
    def parse(self, response):
        """Parsing page links for this season."""
        season_pages = response.xpath("//div[@class='filter']/*/a")  # selects all anchors for months of the season
        yield from response.follow_all(season_pages[0], callback=self.parse_game_data)  # generate requests for all pages automatically    
    
    # TODO: extend this to also extract additional game data...
    def parse_game_data(self, response):
        """Parsing game data from a single page."""
        
        def extract_date(raw_date, read_format="%a, %b %d, %Y", save_format="%Y-%m-%d"):
            """Extract date in nicer format. Check python datetime docs for formating options."""
            date = dt.datetime.strptime(raw_date, read_format)
            return date.strftime(save_format)
            
        def extract_time(raw_time, save_format="%H:%M:%S"):
            """Convert time to military time. Check python datetime docs for formating options."""
            splt = raw_time.split(':')
            padd = splt[0].zfill(2)+':'+splt[1]+'m'  # make hours zero padded
            time = dt.datetime(padd, "%I:%M%p")            
            return time.strftime(save_format)
            
        games = response.xpath("//*/table[@id='schedule']/tbody/*")  # select all games data in the table
        for game in games:
            # data is arranged on different levels, check HTML structure of table on website
            lvl1 = game.xpath("*/text()").extract()  # contains time, scores and attendance
            lvl2 = game.xpath("*/*/text()").extract()  # contains date and team names

            yield {'date': extract_date(lvl2[0]),
                   'time': extract_time(lvl1[0]),
                   'day': extract_date(lvl2[0], save_format="%A"),  # %A means weekday
                   'home_team': lvl2[1],
                   'home_score': int(lvl1[1]),
                   'away_team': lvl2[2],
                   'away_score': int(lvl1[2]),
                   'attendance': int(lvl1[-1].replace(",", '')),
                   }