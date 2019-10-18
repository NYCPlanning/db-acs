import requests
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import math
import json
import os

# e --> estimate, m --> moe
def get_e(e):
    return sum(e)

def get_m(m):
    result = math.sqrt(sum(map(lambda x: x**2, m)))
    return result

def get_c(e, m): 
    if e == 0:
        return ''
    else:
        return m/1.645/e*100

def get_p(e, agg_e):
    if agg_e == 0: 
        return ''
    else:
        return e/agg_e*100

def get_z(e, m, p, agg_e, agg_m):
    if p == 0:
        return ''
    elif p == 100:
        return ''
    elif agg_e == 0:
        return ''
    elif m**2 - (e*agg_m/agg_e)**2 <0:
        return math.sqrt(m**2 + (e*agg_m/agg_e)**2)/agg_e*100
    else: 
        return math.sqrt(m**2 - (e*agg_m/agg_e)**2)/agg_e*100

def find_total(variable, stat='E'):
    if variable[0] == 'B' or variable[0] == 'C' : 
        return f"{variable.split('_')[0]}_001{stat}"
    elif variable[0] == 'D': 
        return f"{variable.split('_')[0]}_0001{stat}"
    else: #S1810_C01_001M
        return f"{'_'.join(variable.split('_')[:2])}_001{stat}"

def calculate(category):
    df = pd.read_csv(f'data/{category}_intermediate.csv', index_col=False)
    dff = df.values
    all_columns = list(df.columns)

    with open(f'data/{category}_meta_lookup.json', 'r') as f:
        meta_lookup = json.load(f)

    for i in tqdm(meta_lookup.keys()):
        variables = meta_lookup[i]
        e_variables = list(map(lambda x: all_columns.index(f'{x}E'), variables))
        m_variables = list(map(lambda x: all_columns.index(f'{x}M'), variables))
        total_e = find_total(variables[0], 'E')
        total_m = find_total(variables[0], 'M')

        df.loc[:,f'{i}E'] = np.apply_along_axis(get_e, 1, dff[:, e_variables])
        df.loc[:,f'{i}M'] = np.apply_along_axis(get_m, 1, dff[:, m_variables])
        df.loc[:,f'{i}C'] = df.apply(lambda row: get_c(row[f'{i}E'], row[f'{i}M']), axis=1)
        
        if len(variables) == 1 and f'{variables[0]}PE' in df.columns:
            '''
            If for some of the records PE is already calculated, 
            then take them directly and calculate PE for the rest
            '''
            df.loc[:,f'{i}P'] \
                = df.loc[df[f'{variables[0]}PE'].isna(), :]\
                    .apply(lambda row: get_p(row[f'{i}E'], row[total_e]), axis=1)    
            
            df.loc[:,f'{i}P']\
                = df.loc[~df[f'{variables[0]}PE'].isna(), :]\
                    .loc[:,f'{variables[0]}PE']
        else: 
            df.loc[:,f'{i}P']\
                = df.apply(lambda row: get_p(row[f'{i}E'], row[total_e]), axis=1)

        if len(variables) == 1 and f'{variables[0]}PM' in df.columns:
            '''
            If for some of the records PM is already calculated, 
            then take them directly and calculate PM for the rest
            '''
            df.loc[:,f'{i}Z']\
                = df.loc[df[f'{variables[0]}PM'].isna(), :]\
                    .apply(lambda row: get_z(row[f'{i}E'], 
                                            row[f'{i}M'], 
                                            row[f'{i}P'], 
                                            row[total_e],
                                            row[total_m]), axis=1)

            df.loc[:,f'{i}Z']\
                = df.loc[~df[f'{variables[0]}PM'].isna(), :]\
                    .loc[:,f'{variables[0]}PM']
        else:
            df.loc[:,f'{i}Z']\
                = df.apply(lambda row: get_z(row[f'{i}E'], 
                                            row[f'{i}M'], 
                                            row[f'{i}P'], 
                                            row[total_e],
                                            row[total_m]), axis=1)

    output_cols = sum([[i+'E', i+'M', i+'P', i+'Z', i+'C'] for i in meta_lookup.keys()], []) + ['GEO_ID', 'NAME']
    df[output_cols].to_csv(f'data/{category}_final.csv', index=False)
    os.system(f'echo "{category} is done"')

if __name__ == "__main__":
    # calculate('demo')
    # calculate('hous')
    calculate('econ')
    # calculate('soci')