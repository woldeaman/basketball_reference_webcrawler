"""Gather game and player data from basketball-reference.com"""
# -*- coding: utf-8 -*-
import scrapy
import datetime as dt

class GamesSpider(scrapy.Spider):
    season = '2019'
    name = "br_spider"
    custom_settings = {'FEED_FORMAT': 'csv', 'FEED_URI': f'season_{season}_data.csv'}
    start_urls = [f'https://www.basketball-reference.com/leagues/NBA_{season}_games.html']
    
    def parse(self, response):
        """Parsing page links for this season."""
        season_pages = response.xpath("//div[@class='filter']/*/a")  # selects all anchors for months of the season
        yield from response.follow_all(season_pages, callback=self.parse_game_data)  # generate requests for all pages automatically    
    
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
            time = dt.datetime.strptime(padd, "%I:%M%p")            
            return time.strftime(save_format)
            
        games = response.xpath("//*/table[@id='schedule']/tbody/*")  # select all games data in the table
        for game in games:
            # data is arranged on different levels, check HTML structure of table on website
            date = game.xpath("*[@data-stat='date_game']/a/text()").extract()[0]
            time = game.xpath("*[@data-stat='game_start_time']/text()").extract()[0]
            h_team = game.xpath("*[@data-stat='home_team_name']/a/text()").extract()[0]
            h_score = game.xpath("*[@data-stat='home_pts']/text()").extract()[0]
            a_team = game.xpath("*[@data-stat='visitor_team_name']/a/text()").extract()[0]
            a_score = game.xpath("*[@data-stat='visitor_pts']/text()").extract()[0]
            attendance = games[0].xpath("*[@data-stat='attendance']/text()").extract()[0]

            yield {'date': extract_date(date),
                   'time': extract_time(time),
                   'day': extract_date(date, save_format="%A"),  # %A means weekday
                   'home_team': h_team,
                   'home_score': int(h_score),
                   'away_team': a_team,
                   'away_score': int(a_score),
                   'attendance': int(attendance.replace(",", '')),
                   }