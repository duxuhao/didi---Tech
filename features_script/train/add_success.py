# -*- coding: utf-8 -*-
"""
Created on Thu Jun 09 14:41:12 2016

@author: 21644336
"""

from collections import defaultdict
import pandas as pd
from multiprocessing import Pool
from sklearn.utils.validation import check_array
import numpy as np

def change_hash(x):
    y = range(len(x.unique()))
    return y

pool = Pool(4)

for name in ['22','24','26','28','30']: #['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21'][10:]:
    print 'date: ' + name
    filename =  'order_data_2016-01-' + name + '_test.csv'
    test = pd.read_csv(filename)
    p_index = change_hash(test.passenger_id)
    dest_index = change_hash(test.dest_district_hash)
    test.replace(test.dest_district_hash.unique(), dest_index,inplace=True)
    #test.replace(test.passenger_id.unique(), p_index,inplace=True)
    
    test['success'] = pd.Series(np.ones(len(test)), index = test.index)
    test.loc[pd.isnull(test.driver_id),'success'] = 0
    X = test[['dest_district_hash','passenger_id','price','start_district_hash']]
    Y = test[['dest_district_hash','passenger_id','price','start_district_hash','driver_id']]
    x = X.duplicated()
    y = Y.duplicated()
    z = ~y & x
    for i in range(len(X)):
        if z[i]:
            T = (test.dest_district_hash == X.dest_district_hash[i]) & (test.passenger_id == X.passenger_id[i]) & (test.price == X.price[i]) & (test.start_district_hash == X.start_district_hash[i])
            test.loc[T,'success'] = 1
            print i
    test.to_csv('new' + filename, index = None)