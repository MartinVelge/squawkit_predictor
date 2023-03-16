from typing import Literal
import requests
import pandas as pd
import numpy as np
import json
from bs4 import BeautifulSoup
from functools import lru_cache
import cachetools.func
from tqdm.contrib.concurrent import thread_map
# pd.set_option('display.max_colwidth', None)

# Source Parameters
url = 'https://www.pilotjobsnetwork.com/'



@cachetools.func.ttl_cache(maxsize=128, ttl=86400)
def get_airlines_urls(url):

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        if href and 'jobs/' in href:
            airline_url = url + href
            airline_name = link.text.strip()
            yield airline_url, airline_name

@cachetools.func.ttl_cache(maxsize=128, ttl=86400)
def get_airline_tables(airline_tuple):
    airline_url, airline_name = airline_tuple
    try:
        list_tables = pd.read_html(airline_url)
        salary = list_tables[3].iloc[2:4,1].tolist()
        last_update = list_tables[3].iloc[2:4,2].tolist()
        return [airline_url, airline_name] + salary + last_update
    except Exception as e:
        print(e)
        try:
            return list_tables[3]
        except NameError:
            return airline_url

def create_airline_json(url, scrap_from_web: bool = True):
    if scrap_from_web:
        list_salaries = list(thread_map(get_airline_tables, get_airlines_urls(url)))
        with open("data.json", "w") as f:
            f.write(json.dumps(list_salaries))
    else:
        with open("data.json", "r") as f:
            list_salaries = json.load(f.read())
    # Dataframe building
    df_salary = pd.DataFrame([salary for salary in list_salaries if type(salary) == list])
    df_salary.columns = ["URL", "AirlineName", "CaptMax", "CaptMin", "DateCaptMax", "DateCaptMin"]

    df_stack = (df_salary.set_index(['URL', 'AirlineName', 'DateCaptMax', 'DateCaptMin'])
       .rename_axis(['Top/Base'], axis=1)
       .stack(dropna=False) # Put True to remove NaNs
       .reset_index())
    df_stack.columns = ['URL', 'AirlineName', 'DateCaptMax', 'DateCaptMin', 'Top/Base', 'Salary']

    # Create a unique column for date depending on Top/Base column value. Extract Year
    df_stack['Date_reworked'] = np.where(df_stack['Top/Base'] == 'CaptMax', df_stack['DateCaptMax'], df_stack['DateCaptMin'])
    df_stack['Date_reworked'] = pd.to_datetime(df_stack['Date_reworked'])
    df_stack['Year'] = df_stack['Date_reworked'].dt.year
    # df_stack['Year'] = df_stack['Year'].astype('Int64')

    # Split column AirlineName on " - " separator
    df_stack[["DateFromLink", "Country", "Name"]] = df_stack["AirlineName"].str.split(" - ", expand=True)

    # Cleaning columns. Dropping non necessary
    clean_order = ['URL', 'Country', 'Name', 'Year', 'Top/Base', 'Salary']
    df_stack = df_stack[clean_order]

    df_stack = df_stack.fillna("NAN")

    # Json creation
    airline_json = df_stack.to_dict(orient="records")
    return airline_json

if __name__ == "__main__":
    create_airline_json(url, scrap_from_web=False)