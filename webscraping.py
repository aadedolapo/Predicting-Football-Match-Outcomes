import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import argparse
from tqdm import tqdm

from beartype import beartype

import warnings
warnings.filterwarnings('ignore')

@beartype
class SoccerScraper:

    def __init__(self,
                  start_year: str,
                  end_year: str,
                  output: str = "match_data") -> None:
       
        self.start_year = start_year
        self.end_year = end_year
        self.output = output


    @beartype
    def scrape(self) -> str:        
        all_matches = []
        years = list(range(int(self.end_year), int(self.start_year), -1))

        url = "https://fbref.com/en/comps/9/Premier-League-Stats"

        try:
            for year in tqdm(years, desc="Getting inputed years"):
                data = requests.get(url)
                soup = BeautifulSoup(data.text)
                league_table = soup.select('table.stats_table')[0]
                
                links = [l.get("href") for l in league_table.find_all('a')]
                links = [l for l in links if '/squads/' in l]
                team_urls = [f"https://fbref.com{l}" for l in links]
                
                previous_season = soup.select("a.prev")[0].get("href")
                url = f"https://fbref.com{previous_season}"
            
            
                for team_url in tqdm(team_urls, desc="Getting Team's url"):
                    team_name = team_url.split("/")[-1].replace("-Stats","").replace("-"," ")
                    
                    data = requests.get(team_url)
                    matches = pd.read_html(data.text, match="Scores & Fixtures")[0]
                    
                    soup = BeautifulSoup(data.text)
                    links = [l.get("href") for l in soup.find_all('a')]
                    links = [l for l in links if l and 'all_comps/shooting/' in l]
                    data = requests.get(f"https://fbref.com{links[0]}")
                    shooting = pd.read_html(data.text, match="Shooting")[0]
                    shooting.columns = shooting.columns.droplevel()
                    
                    try:
                        team_data = matches.merge(shooting[['Date','Sh','SoT','Dist','FK','PK','PKatt']], on='Date')
                    except ValueError:
                        continue
                                
                    team_data = team_data[team_data["Comp"]=="Premier League"]
                    team_data["Season"] = year
                    team_data["Team"] = team_name
                    
                    all_matches.append(team_data)
                    time.sleep(10)
        except:
            print('Error Occured')

        match_df = pd.concat(all_matches)
        match_df.columns = [c.lower() for c in match_df.columns]
        match_df.to_csv(self.output + ".csv", index=False)
        return "Completed"

    # @beartype
    # def to_dataframe(self) -> pd.DataFrame:
    #     match_df = pd.concat(SoccerScraper.scrape(self.start_year, self.end_year))
    #     match_df.columns = [c.lower() for c in match_df.columns]
    #     match_df.to_csv("testing this.csv", index=False)



def main() -> None:
    parser = argparse.ArgumentParser(description="Scrapes football matches for any season intervals")
    parser.add_argument("-e", "--start_year", required=True, help="Starting year")
    parser.add_argument("-s", "--end_year", required=True, help="Ending year")
    parser.add_argument("-o", "--output", required=False, help="csv file containing scraped football statistics", default="match_data")
    args = parser.parse_args()
    call_func = SoccerScraper(args.end_year,
                              args.start_year,
                              args.output).scrape()


if __name__ == "__main__":
    main()
