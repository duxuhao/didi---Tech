# -*- coding: utf-8 -*-
"""
Created on Fri May 27 15:34:54 2016

@author: 21355188
"""

import pandas as pd
import numpy as np
"""
data = pd.read_csv('train_data.csv')
original_data = pd.read_csv('Traindata4.csv')
effective_data = original_data[['location_index','date','timeslice','made','miss']].copy()
"""
gaps = pd.read_csv('gaps.csv')
gaps.columns = ['date','miss','made','location_index','timeslice']
nearest_3_districts = pd.read_csv('nearest_3_districts.csv')

def temp_func(x,ntimeslice):
    return x+ntimeslice
    
def earlier_time(gaps, d, nearest_3_districts,ntimeslice):
    # find data for nearest district ntimeslice ahead for the d^th district
    if True:
        t = nearest_3_districts[nearest_3_districts.d == d]
        t.index = range(len(t))
        d1 = t.ix[0,'d1']
        d2 = t.ix[0,'d2']
        d3 = t.ix[0,'d3']
        dist1 = t.ix[0,'dist1']
        dist2 = t.ix[0,'dist2']
        dist3 = t.ix[0,'dist3']
        
        dset = gaps[gaps.location_index == d]
        d1set = gaps[gaps.location_index == d1]
        d2set = gaps[gaps.location_index == d2]
        d3set = gaps[gaps.location_index == d3]
        dset.index = range(len(dset))
        d1set.index = range(len(d1set))
        d2set.index = range(len(d2set))
        d3set.index = range(len(d3set)) 
        
        # process d1set d2set and d3set to be merged with dset
        d1set.loc[:,'timeslice'] = d1set.apply(lambda row:temp_func(row['timeslice'],ntimeslice),axis = 1)
        d2set.loc[:,'timeslice'] = d2set.apply(lambda row:temp_func(row['timeslice'],ntimeslice),axis = 1)
        d3set.loc[:,'timeslice'] = d3set.apply(lambda row:temp_func(row['timeslice'],ntimeslice),axis = 1)
        
        d1set.loc[:,'location_index'] = d*pd.Series(np.ones(len(d1set)))
        d2set.loc[:,'location_index'] = d*pd.Series(np.ones(len(d2set)))
        d3set.loc[:,'location_index'] = d*pd.Series(np.ones(len(d3set)))    
        
        # pick only the ['date','location_index','timeslice']
        d1set_c = d1set[['date','location_index','timeslice','miss']].copy()
        d2set_c = d2set[['date','location_index','timeslice','miss']].copy()
        d3set_c = d3set[['date','location_index','timeslice','miss']].copy()
        coln1 = 'earlier'+str(ntimeslice)+'_nearest_gap1'
        coln2 = 'earlier'+str(ntimeslice)+'_nearest_gap2'
        coln3 = 'earlier'+str(ntimeslice)+'_nearest_gap3'
        d1set_c.columns = ['date','location_index','timeslice',coln1]
        d2set_c.columns = ['date','location_index','timeslice',coln2] 
        d3set_c.columns = ['date','location_index','timeslice',coln3]
        
        d1set_c.loc[:,coln1] = d1set_c.loc[:,coln1]/dist1
        d2set_c.loc[:,coln2] = d2set_c.loc[:,coln2]/dist2
        d3set_c.loc[:,coln3] = d3set_c.loc[:,coln3]/dist3
        
        temp1 = pd.merge(dset, d1set_c,on = ['date','location_index','timeslice'],how = 'left')
        temp1 = pd.merge(temp1,d2set_c, on = ['date','location_index','timeslice'],how = 'left')
        temp1 = pd.merge(temp1,d3set_c, on = ['date','location_index','timeslice'],how = 'left')
        temp1 = temp1
        temp1 = temp1.dropna()
        
        return temp1
        
        
        
def nearest3_aheadof3_time(gaps, nearest_3_districts):
    data_by_district = []
    for d in range(1,67):
        temp1 = earlier_time(gaps, d, nearest_3_districts,1)
        temp2 = earlier_time(gaps, d, nearest_3_districts,2)
        temp3 = earlier_time(gaps, d, nearest_3_districts,3)
        temp = pd.merge(temp1,temp2,on = ['date','miss','made','location_index','timeslice'],how = 'inner')
        temp = pd.merge(temp,temp3,on = ['date','miss','made','location_index','timeslice'],how = 'inner')
        data_by_district.append(temp)
        print '*'*50
        print '--------- district {0} completed ---------'.format(d)
    tempdf = pd.concat(data_by_district, ignore_index = True)
    tempdf.index = range(len(tempdf))
    return tempdf
    
near_gaps = nearest3_aheadof3_time(gaps, nearest_3_districts)
near_gaps.to_csv('near_gaps.csv',index = False)
"""
given effective_data the dataframe of:
| location | date | timeslice | miss|
| 1        |  20  |    102    | miss|
"""