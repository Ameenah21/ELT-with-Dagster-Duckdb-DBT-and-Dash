# ELT-with-Dagster-Duckdb-DBT-and-Dash
Steps for the Project
- Create a Project File
- Cd into the directory
- Create a Virtual Environment
```
python -m venv venv
```
- Activate the environment
```
For MacOS and Linux users
source venv/bin/activate

For Windows Command Prompt
venv\Scripts\activate 

For Windows Power Shell
.\venv\Scripts\Activate.ps1 


```
- Install Dagster
```
pip install dagster
```
- Create Dagster Project
 ```
dagster project scaffold --name my-dagster-project (or any name you want for your project

```
- Edit the setup.py
  ```
  from setuptools import find_packages, setup
  
  setup(
      name="sports",
      packages=find_packages(exclude=["sports_tests"]),
      install_requires=[
          "dagster",
          "dagster-cloud",
          "dagster",
          "dbt-duckdb",
          "dash",
          "duckdb",
          "bs4",
          "pandas",
          "requests",
          "dagster-webserver",
          "lxml",
          "dagit",
          "dagster-duckdb" ,
          "dagster-duckdb-pandas"
      ],
      extras_require={"dev": ["dagster-webserver", "pytest"]},
  )
  ```
- Install dependencies in setup.py
```
pip install -e ".[dev]"
```
- Import asset dependencies into asset.py
```
import requests
from bs4 import BeautifulSoup
import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource
from dagster import Definitions
import os
```
- Create your first asset
```
@asset
def league_standing():
    urls = [
    {"url": "https://www.skysports.com/ligue-1-table", "source": "Ligue 1"},
    {"url": "https://www.skysports.com/premier-league-table", "source": "Premier League"},
    {"url": "https://www.skysports.com/la-liga-table", "source": "la liga"},
    {"url": "https://www.skysports.com/bundesliga-table", "source": "Bundesliga"},
    {"url": "https://www.skysports.com/serie-a-table", "source": "Seria A"},
    {"url": "https://www.skysports.com/eredivisie-table", "source": "Eredivisie"},
    {"url": "https://www.skysports.com/scottish-premier-table", "source": "Scottish premiership"}
    ]
    dfs = []

    for url_info in urls:
        url = url_info["url"]
        source = url_info["source"]

        # Send HTTP Request and Parse HTML
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "lxml")

        # Find and Extract Table Headers
        table = soup.find("table", class_="standing-table__table")
        headers = table.find_all("th")
        titles = [i.text for i in headers]

        # Create an Empty DataFrame
        df = pd.DataFrame(columns=titles)

        # Iterate Through Table Rows and Extract Data
        rows = table.find_all("tr")
        for i in rows[1:]:
            data = i.find_all("td")
            row = [tr.text.strip() for tr in data]  # Apply .strip() to remove \n
            l = len(df)
            df.loc[l] = row

        # Add a column for source URL
        df["Source"] = source

        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    football_standing = pd.concat(dfs, ignore_index=True)
    football_standing.to_csv("footballstanding.csv")
```

- start your server
```
dagster dev -f hello-dagster.py
```
- Check your dagster server at http://127.0.0.1:3000/ and materialize your assets. If the materialization is successful, you should see a file in your folder named football_standing.csv

- Create the second task to define the second asset to extract the scores
```
@asset
def get_scores():
    url = "https://www.skysports.com/football-results"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")

    home_team = soup.find_all("span", class_="matches__item-col matches__participant matches__participant--side1")
    x = [name.strip() for i in home_team for name in i.stripped_strings]

    scores = soup.find_all("span", class_="matches__teamscores")
    s = [name.strip().replace('\n\n', '\n') for i in scores for name in i.stripped_strings]
    appended_scores = [f"{s[i]}\n{s[i+1]}".replace('\n', ' ') for i in range(0, len(s), 2)]

    away_team = soup.find_all("span", class_="matches__item-col matches__participant matches__participant--side2")
    y = [name.strip() for i in away_team for name in i.stripped_strings]

    # Make sure all arrays have the same length
    min_length = min(len(x), len(appended_scores), len(y))
    data = {"Home Team": x[:min_length], "Scores": appended_scores[:min_length], "Away Team": y[:min_length]}
    footballscores = pd.DataFrame(data)
    footballscores.to_csv("footscores.csv")
```
# Week 2 Working with DuckDB
We are going to create a DuckDB database, create a connecttion to it and load our data in two tables, namely scores_standing and scores table.
There are two ways of interacting with the duckdb and dagster;DuckDB resource and DuckDB I/O manager. We will be using the resource beacuse we want to interact with our tables using SQL, I/O is more suited for libraries like Pandas, Spark and Polars
- Create a dagster dafinition
current_directory = os.getcwd()
database_file = os.path.join(current_directory, "my_duckdb_database.duckdb")
```
defs = Definitions(
    assets=[league_standing, get_scores, create_scores_table],
    resources={
        "duckdb": DuckDBResource(
            database=database_file)}
)

- Create a duckdb database and our first table

```
@asset
def create_scores_table(duckdb: DuckDBResource) -> None:
    sports_df = pd.read_csv(
        "footscores.csv",
        names=['Unnamed: 0', 
               'Home Team', 
               'Scores', 
               'Away Team'],
            
    )

    with duckdb.get_connection() as conn:
        conn.execute("CREATE TABLE sports.scores AS SELECT * FROM scores_df")
```
- Check if table has been created in database successsfully
Create a testdb.py to check if there are tables in the database
```import duckdb
with duckdb.connect("my_duckdb_database.duckdb") as conn:
    # Specify the table name you want to check
    table_name = "scores"
    
    # Query to check if the table exists
    result = conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    
    # Check if the query returned any rows
    table_exists = len(result.fetchall()) > 0
    
    if table_exists:
        print(f"The table '{table_name}' exists.")
    else:
        print(f"The table '{table_name}' does not exist.")
```
- Create the second table
```
@asset
def create_standings_table(duckdb: DuckDBResource) -> None:
    standings_df = pd.read_csv("standing.csv")

    with duckdb.get_connection() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS standings AS SELECT * FROM standings_df")
```
- check your if the two tables exist in you db
```
# Execute the SQL query to list tables
result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")

# Fetch and print the table names
table_names = result.fetchall()
for table_name in table_names:
    print(table_name[0])
```
- Initialize your dbt project
``` 
dbt init project_name
```

