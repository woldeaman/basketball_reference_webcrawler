"""Gather game and player data from basketball-reference.com"""
# -*- coding: utf-8 -*-
import scrapy
import datetime as dt

def extract_datetime(raw_date, read_format="%a, %b %d, %Y %I:%M%p", save_format="%Y-%m-%dT%H:%M:%S"):
        """Extract date or time in ISO-format. Check python datetime docs for formating options."""
        date = dt.datetime.strptime(raw_date, read_format)
        return date.strftime(save_format)

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
    
    def parse_game_data(self, response):
        """Parsing game data from a single page."""
        games = response.xpath("//*/table[@id='schedule']/tbody/*")  # select all games data in the table
        for game in games[:2]:
            # TODO: use get() instead of extract(), which returns single string instead of list
            # data is arranged on different levels, check HTML structure of table on website
            date = game.xpath("*[@data-stat='date_game']/a/text()").get()
            time_raw = game.xpath("*[@data-stat='game_start_time']/text()").get()
            splt = time_raw.split(':')
            time = splt[0].zfill(2)+':'+splt[1]+'m'  # adding full am/pm note and padding zeros
            date_time = ' '.join([date, time])
            h_team = game.xpath("*[@data-stat='home_team_name']/a/text()").get()
            h_score = game.xpath("*[@data-stat='home_pts']/text()").get()
            a_team = game.xpath("*[@data-stat='visitor_team_name']/a/text()").get()
            a_score = game.xpath("*[@data-stat='visitor_pts']/text()").get()
            attendance = games[0].xpath("*[@data-stat='attendance']/text()").get()
            overtime = game.xpath("*[@data-stat='overtimes']/text()").get()
            notes = game.xpath("*[@data-stat='game_remarks']/text()").get()

            # scrape basic game information from page
            yield {'date': extract_datetime(date_time),
                   'weekday': extract_datetime(date_time, save_format="%A"),  # %A means weekday
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
            yield from response.follow_all(game_details, callback=self.parse_game_details)
            
    def parse_game_details(self, response):
        """Parsing more detailed information for each game."""
        date_time = response.xpath("//*/*[@class='scorebox_meta']/div/text()").extract()[0]  # reading date/time
        info_tables = response.xpath("//div[@class='table_container' and contains(@id, 'game-basic')]")  # gathering tables
        
        # cycle through home/away tables
        for table in info_tables:
            team = table.xpath("*/caption/text()").extract()[0].split("(")[0].strip()  # extract team name
            player_rows = table.xpath("*/*/tr[th[@scope='row']]")[:-1]  # last row is game total stats, skipping it
            # loop through all players, first five are starters
            for i, player in enumerate(player_rows):
                role = 'Starter' if i < 5 else 'Reserve'
                name = player.xpath("*/a/text()").get()
                stats = {stat.attrib['data-stat'].upper(): stat.xpath("text()").get() for stat in player.xpath("td")}
                
                # scrape player specific game data 
                yield {'date': extract_datetime(date_time, read_format="%I:%M %p, %B %d, %Y"),
                       'team': team,
                       'player': name,
                       'role': role,
                       **stats,
                       }
                
                
            
            
            
            
        
        
        
        
        
            
            
            