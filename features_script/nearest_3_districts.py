# -*- coding: utf-8 -*-
"""
Created on Thu May 26 23:03:14 2016

@author: 21355188
"""

import pandas as pd
import numpy as np

distances = pd.read_csv('hefei_distances_graph.csv')
def nearest_districts(distances, d1, thresh_dis):
    # return districts within 10km of the target district
    tdf = distances[((distances.s == d1)|(distances.t == d1)) & (distances['median'] < thresh_dis)].sort_values(by='median').copy()
    return tdf
    
def first8_nearest_districts(distances):
    def pick_d_number(pd_series,d):
        if pd_series[0] == d:
            return pd_series[1]
        else:
            return pd_series[0]
    d_list = [] 
    d1_list = []; d2_list = []; d3_list = [];d4_list = []
    d5_list = []; d6_list = []; d7_list = [];d8_list = []
    dist1_list = []; dist2_list = []; dist3_list = [];dist4_list = []
    dist5_list = []; dist6_list = []; dist7_list = [];dist8_list = []
    for d in range(1,67):
        # pick up the first 3 districts of d
        d_list.append(d)
        n_d = nearest_districts(distances,d,500)
        n_d = n_d[~((n_d.s == d) & (n_d.t == d))].copy()
        n_d.index = range(len(n_d))
        dist1_list.append(n_d.ix[0,'median'])
        dist2_list.append(n_d.ix[1,'median'])
        dist3_list.append(n_d.ix[2,'median'])
        dist4_list.append(n_d.ix[3,'median'])
        dist5_list.append(n_d.ix[4,'median'])
        dist6_list.append(n_d.ix[5,'median'])
        dist7_list.append(n_d.ix[6,'median'])
        dist8_list.append(n_d.ix[7,'median'])
        
		
        d1_list.append(pick_d_number(n_d.ix[0,['s','t']],d))
        d2_list.append(pick_d_number(n_d.ix[1,['s','t']],d)) 
        d3_list.append(pick_d_number(n_d.ix[2,['s','t']],d)) 
        d4_list.append(pick_d_number(n_d.ix[3,['s','t']],d))
        d5_list.append(pick_d_number(n_d.ix[4,['s','t']],d)) 
        d6_list.append(pick_d_number(n_d.ix[5,['s','t']],d)) 
        d7_list.append(pick_d_number(n_d.ix[6,['s','t']],d)) 
        d8_list.append(pick_d_number(n_d.ix[7,['s','t']],d)) 
    return pd.DataFrame({'d':d_list,'d1':d1_list,'d2':d2_list,'d3':d3_list, 'd4':d4_list, \
        'd5':d5_list,'d6':d6_list,'d7':d7_list,'d8':d8_list, \
        'dist1':dist1_list,'dist2':dist2_list,'dist3':dist3_list, 'dist4':dist4_list, \
        'dist5':dist5_list,'dist6':dist6_list,'dist7':dist7_list,'dist8':dist8_list})
        
nearest_8_districts = first8_nearest_districts(distances)
nearest_8_districts.to_csv('nearest_8_districts.csv',index = False)          

        


