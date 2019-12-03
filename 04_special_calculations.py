import requests
import pandas as pd
import numpy as np
import functools 
import math
import json

design_factor = {
    'mdage':1.1,
    'mdhhinc':1.5, 
    'mdfaminc':1.5,
    'mdnfinc':1.5,
    'mdewrk':1.6, 
    'mdemftwrk': 1.6,
    'mdefftwrk':1.6,
    'mdrms':1.5,
    'mdvl':1.4,
    'mdgr':1.6
}

mdage = {'mdpop0t4': [0, 4],
    'mdpop5t9': [5, 9],
    'mdpop10t14': [10, 14],
    'mdpop15t17': [15, 17],
    'mdpop18t19': [18, 19],
    'mdpop20': [20, 20.9999],
    'mdpop21': [21, 21.9999],
    'mdpop22t24': [22, 24],
    'mdpop25t29': [25, 29],
    'mdpop30t34': [30, 34],
    'mdpop35t39': [35, 39],
    'mdpop40t44': [40, 44],
    'mdpop45t49': [45, 49],
    'mdpop50t54': [50, 54],
    'mdpop55t59': [55, 59],
    'mdpop60t61': [60, 61],
    'mdpop62t64': [62, 64],
    'mdpop65t66': [65, 66],
    'mdpop67t69': [67, 69],
    'mdpop70t74': [70, 74],
    'mdpop75t79': [75, 79],
    'mdpop80t84': [80, 84],
    'mdpop85pl': [85, 115]}

mdhhinc = {'mdhhiu10': [0, 9999],
 'mdhhi10t14': [10000, 14999],
 'mdhhi15t19': [15000, 19999],
 'mdhhi20t24': [20000, 24999],
 'mdhhi25t29': [25000, 29999],
 'mdhhi30t34': [30000, 34999],
 'mdhhi35t39': [35000, 39999],
 'mdhhi40t44': [40000, 44999],
 'mdhhi45t49': [45000, 49999],
 'mdhhi50t59': [50000, 59999],
 'mdhhi60t74': [60000, 74999],
 'mdhhi75t99': [75000, 99999],
 'mdhi100t124': [100000, 124999],
 'mdhi125t149': [125000, 149999],
 'mdhi150t199': [150000, 199999],
 'mdhhi200pl': [200000, 9999999]}

mdfaminc = {
 'mdfamiu10': [0, 9999],
 'mdfami10t14': [10000, 14999],
 'mdfami15t19': [15000, 19999],
 'mdfami20t24': [20000, 24999],
 'mdfami25t29': [25000, 29999],
 'mdfami30t34': [30000, 34999],
 'mdfami35t39': [35000, 39999],
 'mdfami40t44': [40000, 44999],
 'mdfami45t49': [45000, 49999],
 'mdfami50t59': [50000, 59999],
 'mdfami60t74': [60000, 74999],
 'mdfami75t99': [75000, 99999],
 'mdfi100t124': [100000, 124999],
 'mdfi125t149': [125000, 149999],
 'mdfi150t199': [150000, 199999],
 'mdfami200pl': [200000, 9999999]}

mdnfinc = {'nfmiu10': [0, 9999],
 'nfmi10t14': [10000, 14999],
 'nfmi15t19': [15000, 19999],
 'nfmi20t24': [20000, 24999],
 'nfmi25t29': [25000, 29999],
 'nfmi30t34': [30000, 34999],
 'nfmi35t39': [35000, 39999],
 'nfmi40t44': [40000, 44999],
 'nfmi45t49': [45000, 49999],
 'nfmi50t59': [50000, 59999],
 'nfmi60t74': [60000, 74999],
 'nfmi75t99': [75000, 99999],
 'nf100t124': [100000, 124999],
 'nf125t149': [125000, 149999],
 'nf150t199': [150000, 199999],
 'nfi200pl': [200000, 9999999]}

mdewrk = {'ernu2pt5k': [0, 2499],
 'ern2pt5t5': [2500, 4999],
 'ern5t7pt5': [5000, 7499],
 'e7pt5t10': [7500, 9999],
 'e10t12pt5': [10000, 12499],
 'e12pt5t15': [12500, 14999],
 'e15t17pt5': [15000, 17499],
 'e17pt5t20': [17500, 19999],
 'e20t22pt5': [20000, 22499],
 'e22pt5t25': [22500, 24999],
 'ern25t30': [25000, 29999],
 'ern30t35': [30000, 34999],
 'ern35t40': [35000, 39999],
 'ern40t45': [40000, 44999],
 'ern45t50': [45000, 49999],
 'ern50t55': [50000, 54999],
 'ern55t65': [55000, 64999],
 'ern65t75': [65000, 74999],
 'ern75t100': [75000, 99999],
 'ern100pl': [100000, 250000]}

mdemftwrk = {'mftu2pt5k': [0, 2499],
 'mft2p5t5': [2500, 4999],
 'mft5t7p5': [5000, 7499],
 'mft7p5t10': [7500, 9999],
 'mf10t12p5': [10000, 12499],
 'mf12p5t15': [12500, 14999],
 'mf15t17p5': [15000, 17499],
 'mf17p5t20': [17500, 19999],
 'mf20t22p5': [20000, 22499],
 'mf22p5t25': [22500, 24999],
 'mft25t30': [25000, 29999],
 'mft30t35': [30000, 34999],
 'mft35t40': [35000, 39999],
 'mft40t45': [40000, 44999],
 'mft45t50': [45000, 49999],
 'mft50t55': [50000, 54999],
 'mft55t65': [55000, 64999],
 'mft65t75': [65000, 74999],
 'mft75t100': [75000, 99999],
 'mft100pl': [100000, 250000]}

mdefftwrk= {'fftu2pt5k': [0, 2499],
 'fft2p5t5': [2500, 4999],
 'fft5t7p5': [5000, 7499],
 'fft7p5t10': [7500, 9999],
 'ff10t12p5': [10000, 12499],
 'ff12p5t15': [12500, 14999],
 'ff15t17p5': [15000, 17499],
 'ff17p5t20': [17500, 19999],
 'ff20t22p5': [20000, 22499],
 'ff22p5t25': [22500, 24999],
 'fft25t30': [25000, 29999],
 'fft30t35': [30000, 34999],
 'fft35t40': [35000, 39999],
 'fft40t45': [40000, 44999],
 'fft45t50': [45000, 49999],
 'fft50t55': [50000, 54999],
 'fft55t65': [55000, 64999],
 'fft65t75': [65000, 74999],
 'fft75t100': [75000, 99999],
 'fft100pl': [100000, 250000]}

mdvl = {'ovlu10': [0, 9999],
 'ovl10t14': [10000, 14999],
 'ovl15t19': [15000, 19999],
 'ovl20t24': [20000, 24999],
 'ovl25t29': [25000, 29999],
 'ovl30t34': [30000, 34999],
 'ovl35t39': [35000, 39999],
 'ovl40t49': [40000, 49999],
 'ovl50t59': [50000, 59999],
 'ovl60t69': [60000, 69999],
 'ovl70t79': [70000, 79999],
 'ovl80t89': [80000, 89999],
 'ovl90t99': [90000, 99999],
 'ov100t124': [100000, 124999],
 'ov125t149': [125000, 149999],
 'ov150t174': [150000, 174999],
 'ov175t199': [175000, 199999],
 'ov200t249': [200000, 249999],
 'ov250t299': [250000, 299999],
 'ov300t399': [300000, 399999],
 'ov400t499': [400000, 499999],
 'ov500t749': [500000, 749999],
 'ov750t999': [750000, 999999],
 'ov1t149m': [1000000, 1499999],
 'ov150t199m': [1500000, 1999999],
 'ov2milpl': [2000000, 5000000]}

mdgr = {'ru100': [0, 99],
 'r100t149': [100, 149],
 'r150t199': [150, 199],
 'r200t249': [200, 249],
 'r250t299': [250, 299],
 'r300t349': [300, 349],
 'r350t399': [350, 399],
 'r400t449': [400, 449],
 'r450t499': [450, 499],
 'r500t549': [500, 549],
 'r550t599': [550, 599],
 'r600t649': [600, 649],
 'r650t699': [650, 699],
 'r700t749': [700, 749],
 'r750t799': [750, 799],
 'r800t899': [800, 899],
 'r900t999': [900, 999],
 'r1kt1249': [1000, 1249],
 'r1250t1p5': [1250, 1499],
 'r1p5t1999': [1500, 1999],
 'r2kt2499': [2000, 2499],
 'r2p5t2999': [2500, 2999],
 'r3kt3499': [3000, 3499],
 'r3500pl': [3500, 9000]}

mdrms = {'rms1': [0, 1499],
 'rms2': [1500, 2499],
 'rms3': [2500, 3499],
 'rms4': [3500, 4499],
 'rms5': [4500, 5499],
 'rms6': [5500, 6499],
 'rms7': [6500, 7499],
 'rms8': [7500, 8499],
 'rms9pl': [8500, 9000]}

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

        p_lower = 50 - se_50
        p_upper = 50 + se_50
        
        try:
            lower_bin = max([cumm_dist.index(i) for i in cumm_dist if i<p_lower])
            upper_bin = max([cumm_dist.index(i) for i in cumm_dist if i<p_upper])
        except: 
            print(f"{row['name']} se50 {round(se_50)} plower {round(p_lower)} phigher {round(p_upper)}")
            return np.nan
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
    df['mdagee'] = df.apply(lambda row: get_median(mdage, row), axis=1)
    df['mdagem'] = df.apply(lambda row: get_median_moe(mdage, row, DF=design_factor['mdage']), axis=1)
    df.to_csv('data/demo_final1.csv', index=False)

    # Economics
    df = pd.read_csv('data/econ_final.csv', index_col=False)
    df.columns = map(str.lower, df.columns)
    df['mdhhince'] = df.apply(lambda row: get_median(mdhhinc, row), axis=1)
    df['mdhhincm'] = df.apply(lambda row: get_median_moe(mdhhinc, row, DF=design_factor['mdhhinc']), axis=1)
    df['mdhhincz'] = np.nan
    df['mdhhincp'] = np.nan

    df['mdfamince'] = df.apply(lambda row: get_median(mdfaminc, row), axis=1)
    df['mdfamincm'] = df.apply(lambda row: get_median_moe(mdfaminc, row, DF=design_factor['mdfaminc']), axis=1)
    df['mdfamincz'] = np.nan
    df['mdfamincp'] = np.nan

    df['mdnfince'] = df.apply(lambda row: get_median(mdnfinc, row), axis=1)
    df['mdnfincm'] = df.apply(lambda row: get_median_moe(mdnfinc, row, DF=design_factor['mdnfinc']), axis=1)
    df['mdnfincz'] = np.nan
    df['mdnfincp'] = np.nan

    df['mdemftwrke'] = df.apply(lambda row: get_median(mdemftwrk, row), axis=1)
    df['mdemftwrkm'] = df.apply(lambda row: get_median_moe(mdemftwrk, row, DF=design_factor['mdemftwrk']), axis=1)
    df['mdemftwrkz'] = np.nan
    df['mdemftwrkp'] = np.nan

    df['mdefftwrke'] = df.apply(lambda row: get_median(mdefftwrk, row), axis=1)
    df['mdefftwrkm'] = df.apply(lambda row: get_median_moe(mdefftwrk, row, DF=design_factor['mdefftwrk']), axis=1)
    df['mdefftwrkz'] = np.nan
    df['mdefftwrkp'] = np.nan

    df['percapince'] = df['agip15ple']/df['pop_6e']
    df['percapincm'] = np.sqrt(df['agip15plm']**2 + (df['agip15ple']*df['pop_6m']/df['pop_6e'])**2)*df['pop_6e']
    df['percapincz'] = np.nan
    df['percapincp'] = np.nan

    df['mntrvtme'] = df['agttme']/(df['wrkr16ple']-df['cw_wrkdhme'])
    df['mntrvtmm'] = 1/df['wrkrnothme']*np.sqrt(df['agttmm']**2+(df['agttme']*df['wrkrnothmm']/df['wrkrnothme'])**2)
    df['mntrvtmz'] = np.nan
    df['mntrvtmp'] = np.nan

    df['mnhhince'] = df['aghhince']/df['hh2e']
    df['mnhhincm'] = 1/df['hh5e']*np.sqrt(df['aghhincm']**2+(df['aghhince']*df['hh5m']/df['hh5e'])**2)
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

    df['hovacrte'] = 100*df['hovacue']/df['vacsalee']
    df['hovacrtm'] = df.apply(lambda row: hovacrtm(row['hovacue'], row['vacsalee'], row['vacsalem'], row['hovacum']), axis=1)
    df['hovacrtz'] = np.nan
    df['hovacrtp'] = np.nan

    df['rntvacrte'] = 100*df['vacrnte']/df['rntvacue']
    df['hovacrtm'] = df.apply(lambda row: hovacrtm(row['hovacue'], row['vacsalee'], row['vacsalem'], row['hovacum']), axis=1)
    df['rntvacrtz'] = np.nan
    df['rntvacrtp'] = np.nan

    df['avghhsooce'] = df['popoochue']/df['oochu1e']
    df['avghhsoocm'] = (df['popoochum']**2 + (df['popoochue']*df['oochu4m']/df['oochu4e'])**2)**0.5/df['oochu4e']
    df['avghhsoocz'] = np.nan
    df['avghhsoocp'] = np.nan

    df['avghhsroce'] = df['poprtochue']/df['rochu1e']
    df['avghhsrocm'] = (df['poprtochum']**2 + (df['poprtochue']*df['rochu2m']/df['rochu2e'])**2)**0.5/df['rochu2e']
    df['avghhsrocz'] = np.nan
    df['avghhsrocp'] = np.nan


    df['mdrmse'] = df.apply(lambda row: get_median(mdrms, row), axis=1)
    df['mdrmsm'] = df.apply(lambda row: get_median_moe(mdrms, row, DF=design_factor['mdrms']), axis=1)
    df['mdrmsz'] = np.nan
    df['mdrmsp'] = np.nan

    df.to_csv('data/hous_final1.csv', index=False)

    # Social
    df = pd.read_csv('data/soci_final.csv', index_col=False)
    df.columns = map(str.lower, df.columns)

    df['avghhsze'] = df['hhpop1e']/df['hh1e']
    df['avghhszm'] = (df['hhpop1m']**2 + (df['hh4m']*df['hhpop1e']/df['hh4e'])**2)**0.5/df['hh4e']
    df['avghhszz'] = np.nan
    df['avghhszp'] = np.nan

    df['avgfmsze'] = df['popinfmse']/df['fam1e']
    df['avgfmszm'] = (df['popinfmsm']**2 + (df['fam3m']*df['popinfmse']/df['fam3e'])**2)**0.5/df['fam3e']
    df['avgfmszz'] = np.nan
    df['avgfmszp'] = np.nan
    df.to_csv('data/soci_final1.csv', index=False)
