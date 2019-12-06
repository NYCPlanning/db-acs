import requests
import pandas as pd
import numpy as np
import functools 
import math
import json
from utils import assign_geogname, assign_geotype, format_geoid, \
                    get_c
from data import design_factor, mdage, mdefftwrk, mdemftwrk, \
                mdewrk, mdfaminc, mdgr, mdhhinc, \
                mdnfinc, mdrms, mdvl

def get_median(buckets, row):
    ordered = list(buckets.keys())
    orderedE = [i+'e' for i in ordered]
    N = row[orderedE].sum()
    C = 0
    i = 0
    while C <= N/2 and i<=len(buckets.keys())-1:
        C += int(row[orderedE[i]])
        i += 1
    i = i-1
    if i == 0:
        median = list(buckets.values())[0][1]
    elif C == 0: 
        median =0
    elif i == len(buckets.keys())-1:
        median = list(buckets.values())[-1][1]
    else: 
        C = C - int(row[orderedE[i]])
        L = buckets[ordered[i]][0]
        F = int(row[orderedE[i]])
        W = buckets[ordered[i]][1] - buckets[ordered[i]][0]
        median = L + (N/2 - C)*W/F
    return median


def get_median_moe(buckets, row, DF=1.1):
    ordered = list(buckets.keys())
    orderedE = [i+'e' for i in ordered]
    B = row[orderedE].sum()
    if B == 0: 
        return np.nan
    else:
        cumm_dist = list(np.cumsum(row[orderedE])/B*100)

        se_50 = DF*(((93/(7*B))*2500))**0.5
        
        if se_50 >= 50:
            return np.nan
        else: 
            p_lower = 50 - se_50
            p_upper = 50 + se_50
            
            lower_bin = min([cumm_dist.index(i) for i in cumm_dist if i > p_lower])
            upper_bin = min([cumm_dist.index(i) for i in cumm_dist if i > p_upper])
            
            if lower_bin >= len(ordered)-1 or upper_bin >= len(ordered)-1:
                return np.nan
            else:
                if lower_bin == upper_bin:
                    A1 = min(buckets[ordered[lower_bin]])
                    A2 = min(buckets[ordered[lower_bin+1]])
                    C1 = cumm_dist[lower_bin-1]
                    C2 = cumm_dist[lower_bin]
                    lowerbound = (p_lower - C1)*(A2-A1)/(C2-C1) + A1 
                    upperbound = (p_upper - C1)*(A2-A1)/(C2-C1) + A1

                else:
                    A1_l = min(buckets[ordered[lower_bin]])
                    A2_l = min(buckets[ordered[lower_bin+1]])
                    C1_l = cumm_dist[lower_bin-1]
                    C2_l = cumm_dist[lower_bin]

                    A1_u = min(buckets[ordered[upper_bin]])
                    A2_u = min(buckets[ordered[upper_bin+1]])
                    C1_u = cumm_dist[upper_bin-1]
                    C2_u = cumm_dist[upper_bin]

                    lowerbound = (p_lower - C1_l)*(A2_l-A1_l)/(C2_l-C1_l) + A1_l 
                    upperbound = (p_upper - C1_u)*(A2_u-A1_u)/(C2_u-C1_u) + A1_u

                return (upperbound - lowerbound)*1.645/2

if __name__ == "__main__":
    # Demographics
    df = pd.read_csv('data/demo_final.csv', index_col=False)
    df.columns = map(str.lower, df.columns)
    df['geoid'] = df['geo_id'].apply(format_geoid)
    df['geotype'] = df['geo_id'].apply(assign_geotype)
    df['geogname'] = df.apply(lambda row: assign_geogname(row['geotype'],row['name'],row['geoid']),  axis=1)

    df['mdagee'] = df.apply(lambda row: get_median(mdage, row), axis=1)
    df['mdagem'] = df.apply(lambda row: get_median_moe(mdage, row, DF=design_factor['mdage']), axis=1)
    df['mdagec'] = df.apply(lambda row: get_c(row['mdagee'], row['mdagem']), axis=1)
    df['mdagez'] = np.nan
    df['mdagep'] = np.nan
    df.to_csv('data/demo_final1.csv', index=False)

    # Economics
    df = pd.read_csv('data/econ_final.csv', index_col=False)
    df.columns = map(str.lower, df.columns)
    df['geoid'] = df['geo_id'].apply(format_geoid)
    df['geotype'] = df['geo_id'].apply(assign_geotype)
    df['geogname'] = df.apply(lambda row: assign_geogname(row['geotype'],row['name'],row['geoid']),  axis=1)

    df.loc[df.geotype=='NTA2010','mdhhince'] = df.apply(lambda row: get_median(mdhhinc, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdhhincm'] = df.apply(lambda row: get_median_moe(mdhhinc, row, DF=design_factor['mdhhinc']), axis=1)
    df['mdhhincc'] = df.apply(lambda row: get_c(row['mdhhince'], row['mdhhincm']), axis=1)
    df['mdhhincz'] = np.nan
    df['mdhhincp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdfamince'] = df.apply(lambda row: get_median(mdfaminc, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdfamincm'] = df.apply(lambda row: get_median_moe(mdfaminc, row, DF=design_factor['mdfaminc']), axis=1)
    df['mdfamincc'] = df.apply(lambda row: get_c(row['mdfamince'], row['mdfamincm']), axis=1)
    df['mdfamincz'] = np.nan
    df['mdfamincp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdnfince'] = df.apply(lambda row: get_median(mdnfinc, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdnfincm'] = df.apply(lambda row: get_median_moe(mdnfinc, row, DF=design_factor['mdnfinc']), axis=1)
    df['mdfamincc'] = df.apply(lambda row: get_c(row['mdfamince'], row['mdfamincm']), axis=1)
    df['mdnfincz'] = np.nan
    df['mdnfincp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdewrke'] = df.apply(lambda row: get_median(mdewrk, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdewrkm'] = df.apply(lambda row: get_median_moe(mdewrk, row, DF=design_factor['mdewrk']), axis=1)
    df['mdewrkc'] = df.apply(lambda row: get_c(row['mdewrke'], row['mdewrkm']), axis=1)
    df['mdewrkz'] = np.nan
    df['mdewrkp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdemftwrke'] = df.apply(lambda row: get_median(mdemftwrk, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdemftwrkm'] = df.apply(lambda row: get_median_moe(mdemftwrk, row, DF=design_factor['mdemftwrk']), axis=1)
    df['mdemftwrkc'] = df.apply(lambda row: get_c(row['mdemftwrke'], row['mdemftwrkm']), axis=1)
    df['mdemftwrkz'] = np.nan
    df['mdemftwrkp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdefftwrke'] = df.apply(lambda row: get_median(mdefftwrk, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdefftwrkm'] = df.apply(lambda row: get_median_moe(mdefftwrk, row, DF=design_factor['mdefftwrk']), axis=1)
    df['mdefftwrkc'] = df.apply(lambda row: get_c(row['mdefftwrke'], row['mdefftwrkm']), axis=1)
    df['mdefftwrkz'] = np.nan
    df['mdefftwrkp'] = np.nan

    # For the following special calculation, only apply to NTA
    df.loc[df.geotype=='NTA2010','percapince'] = df['agip15ple']/df['pop_6e']
    df.loc[df.geotype=='NTA2010','percapincm'] = np.sqrt(df['agip15plm']**2 + (df['agip15ple']*df['pop_6m']/df['pop_6e'])**2)*df['pop_6e']
    df.loc[df.geotype=='NTA2010','percapincc'] = df.apply(lambda row: get_c(row['percapince'], row['percapincm']), axis=1)
    df['percapincz'] = np.nan
    df['percapincp'] = np.nan

    df.loc[df.geotype=='NTA2010','mntrvtme'] = df['agttme']/(df['wrkr16ple']-df['cw_wrkdhme'])
    df.loc[df.geotype=='NTA2010','mntrvtmm'] = 1/df['wrkrnothme']*np.sqrt(df['agttmm']**2+(df['agttme']*df['wrkrnothmm']/df['wrkrnothme'])**2)
    df.loc[df.geotype=='NTA2010','mntrvtmc'] = df.apply(lambda row: get_c(row['mntrvtme'], row['mntrvtmm']), axis=1)
    df.loc[df.geotype=='NTA2010','mntrvtmz'] = np.nan
    df.loc[df.geotype=='NTA2010','mntrvtmp'] = np.nan

    df.loc[df.geotype=='NTA2010','mnhhince'] = df['aghhince']/df['hh2e']
    df.loc[df.geotype=='NTA2010','mnhhincm'] = 1/df['hh5e']*np.sqrt(df['aghhincm']**2+(df['aghhince']*df['hh5m']/df['hh5e'])**2)
    df.loc[df.geotype=='NTA2010','mnhhincc'] = df.apply(lambda row: get_c(row['mnhhince'], row['mnhhincm']), axis=1)
    df['mnhhincz'] = np.nan
    df['mnhhincp'] = np.nan

    df['cni1864_2z'] = np.nan
    df['cni1864_2p'] = np.nan

    df['cvlf18t64p'] = np.nan
    df['cvlf18t64z'] = np.nan

    df.to_csv('data/econ_final1.csv', index=False)

    # Housing
    def hovacrtm(hovacue, vacsalee, vacsalem, hovacum):
        if hovacue == 0:
            return 0
        elif vacsalee == 0:
            return 0
        elif vacsalem**2 - (vacsalee*hovacum/hovacue)**2 <0:
            return math.sqrt(vacsalem**2 + (vacsalee*hovacum/hovacue)**2)/hovacue*100
        else: 
            return math.sqrt(vacsalem**2 - (vacsalee*hovacum/hovacue)**2)/hovacue*100

    df = pd.read_csv('data/hous_final.csv', index_col=False)
    df.columns = map(str.lower, df.columns)
    df['geoid'] = df['geo_id'].apply(format_geoid)
    df['geotype'] = df['geo_id'].apply(assign_geotype)
    df['geogname'] = df.apply(lambda row: assign_geogname(row['geotype'],row['name'],row['geoid']),  axis=1)
    
    df.loc[df.geotype=='NTA2010','hovacrte'] = 100*df['hovacue']/df['vacsalee']
    df.loc[df.geotype=='NTA2010','hovacrtm'] = df.apply(lambda row: hovacrtm(row['hovacue'], row['vacsalee'], row['vacsalem'], row['hovacum']), axis=1)
    df.loc[df.geotype=='NTA2010','hovacrtc'] = df.apply(lambda row: get_c(row['hovacrte'], row['hovacrtm']), axis=1)
    df['hovacrtz'] = np.nan
    df['hovacrtp'] = np.nan

    df.loc[df.geotype=='NTA2010','rntvacrte'] = 100*df['vacrnte']/df['rntvacue']
    df.loc[df.geotype=='NTA2010','rntvacrtm'] = df.apply(lambda row: hovacrtm(row['rntvacue'], row['vacrnte'], row['vacrntm'], row['rntvacum']), axis=1)
    df.loc[df.geotype=='NTA2010','rntvacrtc'] = df.apply(lambda row: get_c(row['rntvacrte'], row['rntvacrtm']), axis=1)
    df['rntvacrtz'] = np.nan
    df['rntvacrtp'] = np.nan

    df.loc[df.geotype=='NTA2010','avghhsooce'] = df['popoochue']/df['oochu1e']
    df.loc[df.geotype=='NTA2010','avghhsoocm'] = (df['popoochum']**2 + (df['popoochue']*df['oochu4m']/df['oochu4e'])**2)**0.5/df['oochu4e']
    df.loc[df.geotype=='NTA2010','avghhsoocc'] = df.apply(lambda row: get_c(row['avghhsooce'], row['avghhsoocm']), axis=1)
    df['avghhsoocz'] = np.nan
    df['avghhsoocp'] = np.nan

    df.loc[df.geotype=='NTA2010','avghhsroce'] = df['poprtochue']/df['rochu1e']
    df.loc[df.geotype=='NTA2010','avghhsrocm'] = (df['poprtochum']**2 + (df['poprtochue']*df['rochu2m']/df['rochu2e'])**2)**0.5/df['rochu2e']
    df.loc[df.geotype=='NTA2010','avghhsrocc'] = df.apply(lambda row: get_c(row['avghhsroce'], row['avghhsrocm']), axis=1)
    df['avghhsrocz'] = np.nan
    df['avghhsrocp'] = np.nan


    df.loc[df.geotype=='NTA2010','mdrmse'] = df.apply(lambda row: get_median(mdrms, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdrmsm'] = df.apply(lambda row: get_median_moe(mdrms, row, DF=design_factor['mdrms']), axis=1)
    df['mdrmsc'] = df.apply(lambda row: get_c(row['mdrmse'], row['mdrmsm']), axis=1)
    df['mdrmsz'] = np.nan
    df['mdrmsp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdgre'] = df.apply(lambda row: get_median(mdgr, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdgrm'] = df.apply(lambda row: get_median_moe(mdgr, row, DF=design_factor['mdgr']), axis=1)
    df['mdgrc'] = df.apply(lambda row: get_c(row['mdgre'], row['mdgrm']), axis=1)
    df['mdgrz'] = np.nan
    df['mdgrp'] = np.nan

    df.loc[df.geotype=='NTA2010','mdvle'] = df.apply(lambda row: get_median(mdvl, row), axis=1)
    df.loc[df.geotype=='NTA2010','mdvlm'] = df.apply(lambda row: get_median_moe(mdvl, row, DF=design_factor['mdvl']), axis=1)
    df['mdvlc'] = df.apply(lambda row: get_c(row['mdvle'], row['mdvlm']), axis=1)
    df['mdvlz'] = np.nan
    df['mdvlp'] = np.nan
    df.to_csv('data/hous_final1.csv', index=False)

    # Social
    df = pd.read_csv('data/soci_final.csv', index_col=False)
    df.columns = map(str.lower, df.columns)
    df['geoid'] = df['geo_id'].apply(format_geoid)
    df['geotype'] = df['geo_id'].apply(assign_geotype)
    df['geogname'] = df.apply(lambda row: assign_geogname(row['geotype'],row['name'],row['geoid']),  axis=1)

    df.loc[df.geotype=='NTA2010','avghhsze'] = df['hhpop1e']/df['hh1e']
    df.loc[df.geotype=='NTA2010','avghhszm'] = (df['hhpop1m']**2 + (df['hh4m']*df['hhpop1e']/df['hh4e'])**2)**0.5/df['hh4e']
    df.loc[df.geotype=='NTA2010','avghhszc'] = df.apply(lambda row: get_c(row['avghhsze'], row['avghhszm']), axis=1)
    df['avghhszz'] = np.nan
    df['avghhszp'] = np.nan

    df.loc[df.geotype=='NTA2010','avgfmsze'] = df['popinfmse']/df['fam1e']
    df.loc[df.geotype=='NTA2010','avgfmszm'] = (df['popinfmsm']**2 + (df['fam3m']*df['popinfmse']/df['fam3e'])**2)**0.5/df['fam3e']
    df.loc[df.geotype=='NTA2010','avgfmszc'] = df.apply(lambda row: get_c(row['avgfmsze'], row['avgfmszm']), axis=1)
    df['avgfmszz'] = np.nan
    df['avgfmszp'] = np.nan
    df.to_csv('data/soci_final1.csv', index=False)
