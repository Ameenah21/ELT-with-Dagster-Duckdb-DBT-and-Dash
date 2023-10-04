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
