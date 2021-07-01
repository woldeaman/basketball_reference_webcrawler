"""Gather game and player data from basketball-reference.com"""
# -*- coding: utf-8 -*-
import scrapy
import datetime as dt

class BR_Data_Spider(scrapy.Spider):
    name = "basketball-reference"
    year = dt.date.today().strftime("%Y")  # get current year for standart season
    custom_settings = {'FEED_FORMAT': 'csv', 'FEED_URI': f'game_data.csv'}
    
    def __init__(self, season=year, *args, **kwargs):
        super(BR_Data_Spider, self).__init__(*args, **kwargs)  # pass all arguments to standart class
        self.start_urls = [f'https://www.basketball-reference.com/leagues/NBA_{season}_games.html']
        
    def parse(self, response):
        """Parsing page links for this season."""
        season_pages = response.xpath("//div[@class='filter']/*/a")  # selects all anchors for months of the season
        yield from response.follow_all(season_pages, callback=self.parse_game_data)  # generate requests for all pages automatically    
    
    def _extract_datetime(raw_date, read_format="%a, %b %d, %Y %I:%M%p", save_format="%Y-%m-%dT%H:%M:%S"):
        """Extract date or time in ISO-format. Check python datetime docs for formating options."""
            date = dt.datetime.strptime(raw_date, read_format)
            return date.strftime(save_format)
            
    def parse_game_data(self, response):
        """Parsing game data from a single page."""
        games = response.xpath("//*/table[@id='schedule']/tbody/*")  # select all games data in the table
        for game in games:
            # data is arranged on different levels, check HTML structure of table on website
            date = game.xpath("*[@data-stat='date_game']/a/text()").extract()[0]
            time_raw = game.xpath("*[@data-stat='game_start_time']/text()").extract()[0]
            splt = time_raw.split(':')
            time = splt[0].zfill(2)+':'+splt[1]+'m'  # adding full am/pm note and padding zeros
            date_time = ' '.join([date, time])
            h_team = game.xpath("*[@data-stat='home_team_name']/a/text()").extract()[0]
            h_score = game.xpath("*[@data-stat='home_pts']/text()").extract()[0]
            a_team = game.xpath("*[@data-stat='visitor_team_name']/a/text()").extract()[0]
            a_score = game.xpath("*[@data-stat='visitor_pts']/text()").extract()[0]
            attendance = games[0].xpath("*[@data-stat='attendance']/text()").extract()[0]
            overtime = game.xpath("*[@data-stat='overtimes']/text()").extract()
            overtime = overtime[0] if len(overtime) > 0 else None
            notes = game.xpath("*[@data-stat='game_remarks']/text()").extract()
            notes = notes[0] if len(notes) > 0 else None

            # scrape basic game information from page
            yield {'date': self._extract_datetime(date_time),
                   'day': self._extract_datetime(date_time, save_format="%A"),  # %A means weekday
                   'home_team': h_team,
                   'home_score': int(h_score),
                   'away_team': a_team,
                   'away_score': int(a_score),
                   'attendance': int(attendance.replace(",", '')),
                   'overtime': overtime,
                   'remarks': notes,
                   }
            
            # yield request for additional detailed game data
            game_details = game.xpath("*/a[text()='Box Score']")
            yield response.follow(game_details, callback=self.parse_game_details)
