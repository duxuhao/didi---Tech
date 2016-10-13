# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 20:17:06 2016

@author: 21355188
"""

import numpy as np
import pandas as pd
from multiprocessing import Pool

pool = Pool(8)





def is_null_order(driver_id):
    if type(driver_id) == str:
        return 1
    elif type(driver_id) == float:
        return 0
        
def make_minute_slot(time_of_a_day):
    t = time_of_a_day.split(':')
    hour = float(t[0])
    minute = float(t[1])
    return 60*hour+minute    
    
def decay_weighted_gap(time_diff,half_life = 10):
    Lambda = np.log(2)/half_life
    return np.exp(-Lambda*time_diff)
    
def preprocess_data(temp):
    """
    replace the start_district_hash with district_id
    replace driver_id with successful_order
    replace time_of_a_day with time_slot
    """
    cluster_map = pd.read_csv('cluster_map.csv')
    cluster_map.columns = ['start_district_hash','district_id']
    temp = pd.merge(temp, cluster_map, on='start_district_hash', how ='inner')
    temp.ix[:,'successful_order']= 1
    temp.ix[:,'successful_order']=temp.apply(lambda row:is_null_order(row['driver_id']),axis = 1)
    temp.ix[:,'date'] = float(dates[dn])
    temp = temp[['date','district_id','time_of_a_day','successful_order']].copy()
    temp.ix[:,'time_slot'] = 1
    temp.ix[:,'time_slot'] = temp.apply(lambda row:make_minute_slot(row['time_of_a_day']),axis = 1)
    temp = temp[['date','district_id','time_slot','successful_order']].copy()
    return temp


def get_day_gaps(temp,dn):
    day_gaps = pd.DataFrame(columns = ['date','district_id','time_slot','gap_5min','gap_10min'])
    for dis_n in range(1,67):
        for tn in range(10,1440):
            #print '-'*50;
            #print '-'*10 +'district: '+str(dis_n)+' | time_slot: '+str(tn)+' |'+'-'*10;
            #dis_n =23 # district dis_n        
            #tn = 1000 # tn
            mask = (temp['district_id']==dis_n) & (temp['date']==float(dates[dn])) & (temp['time_slot']>= tn-30) & (temp['time_slot']<=tn-1)
            ttemp = temp[mask]
            mask2 = (ttemp['successful_order'] ==0)
            ttemp2 = ttemp[mask2].copy()
            ttemp2['time_slot'] = tn - ttemp2['time_slot']
            temp_list = list(ttemp2['time_slot']) 
            gap5 = np.sum([decay_weighted_gap(x,half_life = 5) for x in temp_list])
            #gap10 = np.sum([decay_weighted_gap(x,half_life = 10) for x in temp_list])
            gap10 = 100
            day_gaps.loc[len(day_gaps),:] = [dn+1, dis_n, tn, gap5,gap10]
    return day_gaps
            
        

dates = range(1,22)
def make_date_str(a_date):
    if len(str(a_date)) == 1:
        return '0'+str(a_date)
    else:
        return str(a_date)
dates = [make_date_str(x) for x in dates]
gaps_days_list = []
#for n in range(21):
for dn in range(0,21):
    temp = pd.read_csv('order_data_2016-01-'+dates[dn]+'.csv')
    
    temp = temp[['date','start_district_hash','time_of_a_day','driver_id']]
    print '*'*50;print '--------start prepare data for date '+str(dates[dn])+ '  -------'; print '*'*50;
    temp = preprocess_data(temp)
    gaps_days_list.append(get_day_gaps(temp,dn))
    
gaps_decay = pd.concat(gaps_days_list,axis = 0)
gaps_decay.to_csv('gaps_decay_normal.csv',index = False)

