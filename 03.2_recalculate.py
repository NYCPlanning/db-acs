import pandas as pd
import numpy as np
import math
import json
import os
from utils import get_p, get_z, get_c

# Import tables as outputted from 03.1_special_calculations.py
demo = pd.read_csv('data/demo_final1.csv', index_col=False, low_memory=False)
econ = pd.read_csv('data/econ_final1.csv', index_col=False, low_memory=False)
soci = pd.read_csv('data/soci_final1.csv', index_col=False, low_memory=False)
hous = pd.read_csv('data/hous_final1.csv', index_col=False, low_memory=False)
base = json.load(open('data/base_lookup.json'))

def recalculate(df, base_var):
    '''
    Recalculates proportion estimate and MOE calculations
    in case special calculations have changed the denominators.

    Parameters
    ----------
    df: pd DataFrame
       Contains results from special calculations

    base_var: 
        Variable to recalculate

    Returns
    -------
    df: pd DataFrame
        Table with recalculated proportion est and MOE
        
    '''
    variables = [a.lower() for a,b in base.items() if b.lower() == base_var]
    if len(variables) == 0:
        pass
    else:
        for i in variables:
            df.loc[:,f'{i}p']\
                = df.apply(lambda row: get_p(row[f'{i}e'], row[f'{base_var}e']), axis=1)
            df.loc[:,f'{i}z']\
                = df.apply(lambda row: get_z(row[f'{i}e'], 
                                            row[f'{i}m'], 
                                            row[f'{i}p'], 
                                            row[f'{base_var}e'],
                                            row[f'{base_var}m']), axis=1)
    print(f'variables with base {base_var} is recalcuated')
    return df

"""
 _____ ____ ___  _   _ 
| ____/ ___/ _ \| \ | |
|  _|| |  | | | |  \| |
| |__| |__| |_| | |\  |
|_____\____\___/|_| \_|

"""
econ = recalculate(econ, 'mdhhinc')
econ = recalculate(econ, 'mdfaminc')
econ = recalculate(econ, 'mdnfinc')
econ = recalculate(econ, 'mdewrk')
econ = recalculate(econ, 'mdemftwrk')
econ = recalculate(econ, 'mdefftwrk')
econ = recalculate(econ, 'percapinc')
econ = recalculate(econ, 'mntrvtm')
econ = recalculate(econ, 'mnhhinc')
econ.to_csv('data/econ_final2.csv', index=False)
"""
 ____  _____ __  __  ___  
|  _ \| ____|  \/  |/ _ \ 
| | | |  _| | |\/| | | | |
| |_| | |___| |  | | |_| |
|____/|_____|_|  |_|\___/ 

"""
demo = recalculate(demo, 'mdage')
demo.to_csv('data/demo_final2.csv', index=False)

"""
 _   _  ___  _   _ ____  
| | | |/ _ \| | | / ___| 
| |_| | | | | | | \___ \ 
|  _  | |_| | |_| |___) |
|_| |_|\___/ \___/|____/ 
"""
hous = recalculate(hous, 'hovacrt')
hous = recalculate(hous, 'rntvacrt')
hous = recalculate(hous, 'avghhsooc')
hous = recalculate(hous, 'avghhsroc')
hous = recalculate(hous, 'mdrms')
hous = recalculate(hous, 'mdgr')
hous = recalculate(hous, 'mdvl')
hous.to_csv('data/hous_final2.csv', index=False)


"""
 ____   ___   ____ ___ 
/ ___| / _ \ / ___|_ _|
\___ \| | | | |    | | 
 ___) | |_| | |___ | | 
|____/ \___/ \____|___|
"""
soci = recalculate(soci, 'avgfmsz')
soci = recalculate(soci, 'avghhsz')
soci.loc[soci.ea_bchdhe.isna(), 'ea_bchdhe'] = soci.loc[soci.ea_bchdhe.isna(), :]\
                                                    .apply(lambda row: row['ea_bchdhm']*row['ea_bchdhp'], axis=1)
soci.loc[:, 'ea_bchdhc'] = soci.apply(lambda row: get_c(row['ea_bchdhe'], row['ea_bchdhm']), axis=1)
soci.loc[:, 'ea_bchdhz'] = soci.apply(lambda row: get_z(row['ea_bchdhe'],
                                            row['ea_bchdhm'],
                                            row['ea_bchdhp'], 
                                            row['ea_p25ple'],
                                            row['ea_p25plm']), axis=1)
soci.to_csv('data/soci_final2.csv', index=False)