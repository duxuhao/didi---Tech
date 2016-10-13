# -*- coding: utf-8 -*-
"""
Created on Sat Jun 11 00:00:00 2016

@author: 21644336
"""

import pandas as pd
import numpy as np

df = pd.read_csv('temperature_144.csv')
df2 = pd.read_csv('base_144.csv')
df2['change']=pd.Series(np.zeros(len(df)),index = df2.index)
df2['temperature']=pd.Series(df.temperature_10,index = df2.index)
df2.date.replace(df2.date.unique(),range(1,22),inplace = True)
df2['t'] = pd.Series((df2.date-1)*288+df2.timeslice,index = df2.index)
X = df2.copy()
X.t += 1
X = X[['district_id','t','temperature']]
XX = pd.merge(df2,X,on=['district_id','t'],how = 'left',left_index =False )
XX.change = XX.temperature_y - XX.temperature_x
XX.loc[pd.isnull(XX.change),'change'] = 0
w_change = XX[['change']]
w_change.columns = ['temperature_change_144']
w_change.to_csv('temperature_change_144.csv',index = None)