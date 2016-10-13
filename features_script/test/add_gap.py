# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 11:51:10 2016

@author: 21644336
"""

from collections import defaultdict
import pandas as pd
from multiprocessing import Pool
from sklearn.utils.validation import check_array
import numpy as np

Mymap = pd.read_csv('cluster_map.csv')
All = []
for name in ['10','15','16','17','21']:
    print 'date: ' + name
    filename1 =  'order_data_2016-01-' + name + '.csv'
    filename2 =  'neworder_data_2016-01-' + name + '.csv'
    test = pd.read_csv(filename2)
    df = pd.read_csv(filename1)
    df['success']=pd.Series(test.success, index = df.index)
    use = df[['start_district_hash','time_of_a_day','success']]
    T = []
    for i in use.time_of_a_day:
        T.append(int(i.split(':')[0])*60+int(i.split(':')[1])+1)
    
    use['timeslice'] = pd.Series(T, index = use.index)
    use.replace(list(Mymap.district_hash), list(Mymap.district_id),inplace = True)
    
    T = []
    for l in range(1,67):
        print l
        for t in range(1,1441):
            X = (use.start_district_hash == l) & (use.timeslice == t) & (use.success == 0)
            Y = (use.start_district_hash == l) & (use.timeslice == t)
            T.append([l, t, len(use[X]), len(use[Y]) - len(use[X])])
            #All.append([name, l, t, len(use[X]), len(use[Y]) - len(use[X])])
    New = pd.DataFrame(T)
    New.columns = ['district_id','timeslice','gap','made']
    New.to_csv(name + 'gap.csv', index = None)
    
        