import requests
import pandas as pd
import json
import os

api_key=os.environ['API_KEY']

demo_groups = ['B01001', 'B05003', 'B02015', 'DP05', 'B03001']

soci_groups = ['B00002', 'B11002', 'B04006', 'B11009', 'B05005', 
                'C16001', 'B05006', 'DP02', 'B07204', 'S1810']
                
econ_groups = ['B00001', 'B19025', 'B00002', 'B19101', 'B01001', 
                'B19201', 'B08013', 'B19313', 'B17010', 'B20005', 
                'B17024', 'DP03', 'B19001', 'S1701']
                
hous_groups = ['B00002', 'B25070', 'B25004', 'B25075', 'B25008', 
              'DP04', 'B25063']

all_groups = demo_groups + soci_groups + econ_groups + hous_groups

groups = json.loads(requests.get('https://api.census.gov/data/2017/acs/acs5/groups.json').content)
profile_groups = json.loads(requests.get('https://api.census.gov/data/2017/acs/acs5/profile/groups.json').content)
cprofile_groups = json.loads(requests.get('https://api.census.gov/data/2017/acs/acs5/cprofile/groups.json').content)
subject_groups = json.loads(requests.get('https://api.census.gov/data/2017/acs/acs5/subject/groups.json').content)

meta = pd.DataFrame(groups['groups']+profile_groups['groups']+cprofile_groups['groups']+subject_groups['groups'])
meta['description'] = meta['description'].apply(lambda x: x.lower())
meta = meta[meta['name'].isin(all_groups)]

endpoint_lookup = {
    'B':'',
    'C':'', 
    'S':'/subject', 
    'D':'/profile'
}
meta['endpoint'] = meta['name'].apply(lambda x: endpoint_lookup.get(x.strip()[0]))


def get_tract(group, endpoint):
    frames = []
    for county in ['081', '085', '005', '047', '061']:
        url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=tract:*&in=state:36&in=county:{county}&key={api_key}'
        resp = requests.request('GET', url).content
        df = pd.DataFrame(json.loads(resp)[1:])
        df.columns = json.loads(resp)[0]
        frames.append(df)
    return pd.concat(frames)

def get_puma(group, endpoint):
    url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=Public Use Microdata Area:*&in=state:36&key={api_key}'
    resp = requests.request('GET', url).content
    df = pd.DataFrame(json.loads(resp)[1:])
    df.columns = json.loads(resp)[0]
    return df[df['NAME'].str.startswith('NYC')]


def get_borough(group, endpoint):
    frames = []
    for county in ['081', '085', '005', '047', '061']:
        url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=county:{county}&in=state:36&key={api_key}'
        resp = requests.request('GET', url).content
        df = pd.DataFrame(json.loads(resp)[1:])
        df.columns = json.loads(resp)[0]
        frames.append(df)
    return pd.concat(frames)

def get_city(group,endpoint):
    url = f'https://api.census.gov/data/2017/acs/acs5{endpoint}?get=group({group})&for=place:51000&in=state:36&key={api_key}'
    resp = requests.request('GET', url).content
    df = pd.DataFrame(json.loads(resp)[1:])
    df.columns = json.loads(resp)[0]
    return df

def create_table(group, getter):
    frames = []
    for i in group:
        df = getter(i, meta.loc[meta['name']==i]['endpoint'].values[0])
        frames.append(df)
    df = frames[0]
    df = df[df.columns.difference(['place', 'tract', 'state', 'county', 'public use microdata area'])]
    for i in frames[1:]:
        df = pd.merge(df, i[i.columns.difference(['state', 'county', 'tract', 'NAME', 'place', 'public use microdata area'])], 
                      left_on='GEO_ID', right_on='GEO_ID')
    df = df[[i for i in list(df.columns) if i[-1:] != 'A']]
    return df

def create_big_table(group):
    frames = []
    frames.append(create_table(group, get_borough))
    frames.append(create_table(group, get_tract))
    frames.append(create_table(group, get_city))
    frames.append(create_table(group, get_puma))
    return pd.concat(frames)

demo_df = create_big_table(demo_groups)
hous_df = create_big_table(hous_groups)
soci_df = create_big_table(soci_groups)
econ_df = create_big_table(econ_groups)

demo_df.to_csv('data/demo.csv', index=False)
# hous_df.to_csv('data/hous.csv', index=False)
# soci_df.to_csv('data/soci.csv', index=False)
# econ_df.to_csv('data/econ.csv', index=False)

