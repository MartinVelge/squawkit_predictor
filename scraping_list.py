import requests
import pandas as pd
import numpy as np
import json
from bs4 import BeautifulSoup
from tqdm.contrib.concurrent import thread_map

def get_airlines_urls():
    url = 'https://www.pilotjobsnetwork.com/'
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        if href and 'jobs/' in href:
            airline_url = url + href
            last_date, country, airline_name = [s.strip() for s in link.text.split(" - ")]
            yield last_date, airline_url, country, airline_name

# cache possible
def get_airline_tables(airline_tuple):
    last_date, airline_url, country, airline_name = airline_tuple
    try:
        list_tables = pd.read_html(airline_url)
        salary = list_tables[3].iloc[2:4,1].tolist()
        last_update = list_tables[3].iloc[2:4,2].tolist()
        return airline_url, airline_name, salary, last_date, country, last_update
    except Exception as e:
        try:
            return e, airline_url, airline_name, list_tables[3], last_date, country, list_tables[3]
        except NameError:
            return e, airline_url, airline_name, None, last_date, country, None

def extract_airlines_from_jobs(jobs):
    return set((job['last_date'], job['airline_url'], job['country'], job['airline_name']) for job in jobs)

def clean_salaries(salaries):
    df_salary = pd.DataFrame([salary for salary in salaries if len(salary) == 6])
    df_salary.columns = ['URL', 'AirlineName', 'CaptMax', 'CaptMin', 'DateCaptMax', 'DateCaptMin']
    print("df_salary:", df_salary) 

    # DF Stack on Top/Base. Requirement for ML model (Olivier)
    df_stack = (df_salary.set_index(('URL', 'AirlineName', 'DateCaptMax', 'DateCaptMin'))
    .rename_axis(['Top/Base'], axis=1)
    .stack(dropna=False) # Put True to remove NaNs
    .reset_index())
    df_stack.columns = ['URL', 'AirlineName', 'DateCaptMax', 'DateCaptMin', 'Top/Base', 'Salary']

    # Year extraction
    df_stack['Date_reworked'] = np.where(df_stack['Top/Base'] == 'CaptMax', df_stack['DateCaptMax'], df_stack['DateCaptMin'])
    df_stack['Date_reworked'] = pd.to_datetime(df_stack['Date_reworked'])
    df_stack['Year'] = df_stack['Date_reworked'].dt.year

    # Split column AirlineName on " - " separator
    df_stack[["DateFromLink", "Country", "Name"]] = df_stack["AirlineName"].str.split(" - ", expand=True)

    # Cleaning columns. Dropping non necessary
    clean_order = ['URL', 'Country', 'Name', 'Last_Date', 'Year', 'Top/Base', 'Salary']
    df_stack = df_stack[clean_order]

    df_stack = df_stack.fillna("NAN")
    return df_stack.to_dict(orient="records")

def get_new_jobs(jobs):
    old_airlines = extract_airlines_from_jobs(jobs)
    new_airlines = set(get_airlines_urls())
    airlines_to_scrape = new_airlines.difference(old_airlines)
    salaries_gen = thread_map(get_airline_tables, airlines_to_scrape)
    return clean_salaries(salaries_gen)

def merge_jobs(old_jobs, new_jobs):
    for new_job in new_jobs:
        for old_job in old_jobs:
            if new_job['URL'] == old_job['URL'] and new_job['Top/Base'] == old_job['Top/Base']:
                old_job = new_job
        old_jobs.append(new_job)
    return old_jobs




if __name__ == "__main__":
    try: 
        with open("jobs.json", "r") as fd:
            old_jobs = json.load(fd)
    except FileNotFoundError:
        old_jobs = []

    new_jobs = get_new_jobs(old_jobs)
    jobs = merge_jobs(old_jobs, new_jobs)

    with open("jobs.json", "w") as fd:
        json.dump(jobs, fd)