import requests
import pandas as pd
import numpy as np
import json
import os
from multiprocessing import Pool, cpu_count

api_key = os.environ['API_KEY']

# Table codes
demo_groups = ['B01001', 'B05003', 'B02015', 'DP05', 'B03001']

soci_groups = ['B00002', 'B11002', 'B04006', 'B11009', 'B05005',
               'C16001', 'B05006', 'DP02', 'B07204', 'S1810']

econ_groups = ['B00001', 'B19025', 'B00002', 'B19101', 'B01001',
               'B19201', 'B08013', 'B19313', 'B17010', 'B20005',
               'B17024', 'DP03', 'B19001', 'S1701']

hous_groups = ['B00002', 'B25070', 'B25004', 'B25075', 'B25008',
               'DP04', 'B25063']

all_groups = demo_groups + soci_groups + econ_groups + hous_groups

# Get dictionaries of table descriptions & locations
groups = json.loads(requests.get(
    'https://api.census.gov/data/2017/acs/acs5/groups.json').content)
profile_groups = json.loads(requests.get(
    'https://api.census.gov/data/2017/acs/acs5/profile/groups.json').content)
cprofile_groups = json.loads(requests.get(
    'https://api.census.gov/data/2017/acs/acs5/cprofile/groups.json').content)
subject_groups = json.loads(requests.get(
    'https://api.census.gov/data/2017/acs/acs5/subject/groups.json').content)

# Create DataFrame compiling descriptions of variables from different table types
meta = pd.DataFrame(groups['groups']+profile_groups['groups'] +
                    cprofile_groups['groups']+subject_groups['groups'])
meta['description'] = meta['description'].apply(lambda x: x.lower())
meta = meta[meta['name'].isin(all_groups)]

# Add endpoint for table types -- refer to different table authors
endpoint_lookup = {
    'B': '',
    'C': '',
    'S': '/subject',
    'D': '/profile'
}
meta['endpoint'] = meta['name'].apply(
    lambda x: endpoint_lookup.get(x.strip()[0]))


def get_tract(group, endpoint):
    '''
    Downloads requested tract-level table for the five
    NYC counties and combines into a single table

    Parameters
    ----------
    group: list of str
        List containing census/ACS table codes
    endpoint: str
         'B', 'C', 'S', or 'D'
         Code for table type
    Returns
    -------
    pd DataFrame
        Contains data from requested ACS 5-year table,
        all five NYC counties included


    '''
    frames = []
    # Download tract-level tables using census API, and combine into a single NYC DataFrame
    for county in ['081', '085', '005', '047', '061']:
        url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=tract:*&in=state:36&in=county:{county}&key={api_key}'
        resp = requests.request('GET', url).content
        df = pd.DataFrame(json.loads(resp)[1:])
        df.columns = json.loads(resp)[0]
        frames.append(df)
    return pd.concat(frames)


def get_puma(group, endpoint):
    '''
    Downloads requested PUMA-level table for NY state,
    and returns only PUMAs in NYC.

    Parameters
    ----------
    group: list of str
        List containing census/ACS table codes
    endpoint: str
         'B', 'C', 'S', or 'D'
         Code for table type

    Returns
    -------
    pd DataFrame
         PUMA-level data from requested ACS 5-year table,
         only NYC PUMAs

    '''
    # Download puma-level tables using census API for PUMAs in NY
    url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=Public Use Microdata Area:*&in=state:36&key={api_key}'
    resp = requests.request('GET', url).content
    df = pd.DataFrame(json.loads(resp)[1:])
    df.columns = json.loads(resp)[0]

    # Only return PUMAs in NYC
    return df[df['NAME'].str.startswith('NYC')]


def get_borough(group, endpoint):
    '''
    Downloads requested county/borough-level table for 
    the five NYC counties and combines into a single table

    Parameters
    ----------
    group: list of str
        List containing census/ACS table codes
    endpoint: str
         'B', 'C', 'S', or 'D'
         Code for table type
    Returns
    -------
    pd DataFrame
        Contains data from requested ACS 5-year table,
        all five NYC counties included


    '''
    frames = []
    # Download tract-level tables using census API, and combine into a single NYC DataFrame
    for county in ['081', '085', '005', '047', '061']:
        url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=county:{county}&in=state:36&key={api_key}'
        resp = requests.request('GET', url).content
        df = pd.DataFrame(json.loads(resp)[1:])
        df.columns = json.loads(resp)[0]
        frames.append(df)
    return pd.concat(frames)


def get_city(group, endpoint):
    '''
    Downloads requested city-level table for NYC

    Parameters
    ----------
    group: list of str
        List containing census/ACS table codes
    endpoint: str
         'B', 'C', 'S', or 'D'
         Code for table type

    Returns
    -------
    df: pd DataFrame
         City-level data from requested ACS 5-year table
         for NYC

    '''
    # Download city-level tables using census API for NYC
    url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=place:51000&in=state:36&key={api_key}'
    resp = requests.request('GET', url).content
    df = pd.DataFrame(json.loads(resp)[1:])
    df.columns = json.loads(resp)[0]
    return df


def create_table(group, getter):
    '''
    Loop through all table codes in a group to download
    ACS tables, then merge by geography into a single table.

    Parameters
    ----------
    group: list of str
        List containing census/ACS table codes

    getter: func
        get_tract, get_puma, get_borough, get_city
        Function for downloading data via API

    Returns
    -------
    df: pd DataFrame
         Table containing all data from a subject group, 
         merged by geography. Excess columns are dropped.       

    '''
    frames = []

    # Loop through table codes in group and download from API
    for i in group:
        df = getter(i, meta.loc[meta['name'] == i]['endpoint'].values[0])
        frames.append(df)
    df = frames[0]  # Reset to first table
    # If overwriting, why not use drop?
    df = df[df.columns.difference(
        ['place', 'tract', 'state', 'county', 'public use microdata area'])]

    # Merge tables by geography, dropping columns with names ending with A
    for i in frames[1:]:
        df = pd.merge(df, i[i.columns.difference(['state', 'county', 'tract', 'NAME', 'place', 'public use microdata area'])],
                      left_on='GEO_ID', right_on='GEO_ID')
    df = df[[i for i in list(df.columns) if i[-1:] != 'A']]
    return df


def create_big_table(group):
    '''
    Takes merged tables from various geographic scales
    and combines into a single table. Different rows 
    contain data from different spatial scales.

    Parameters
    ----------
    group: str
        List containing census/ACS table codes

    Returns
    -------
    pd DataFrame
        Single table containing data for all variables in a 
        given group, over four geographic scales. Scales are
        separated into different rows.      

    '''
    frames = []
    frames.append(create_table(group, get_borough))
    frames.append(create_table(group, get_tract))
    frames.append(create_table(group, get_city))
    frames.append(create_table(group, get_puma))
    return pd.concat(frames)


def download(inputs):
    '''
    Executes data downloading and merging scripts to
    create a csv containing all variables for a specified
    category, across four geographic scales.
    Different scales are different rows in the table.

    Parameters
    ----------
    inputs: dict
        Expects 'category' key, value is string label of subject type.
        Expects 'group' key, value is list containing 
        census/ACS table codes.

    '''
    category = inputs.get('category')
    group = inputs.get('group')
    df = create_big_table(group)
    df.to_csv(f'data/{category}.csv', index=False)
    print(f'{category} is done!')


if __name__ == "__main__":
    with Pool(processes=cpu_count()) as pool:
        pool.map(download, [
            dict(category='demo', group=demo_groups),
            dict(category='econ', group=econ_groups),
            dict(category='soci', group=soci_groups),
            dict(category='hous', group=hous_groups)])
