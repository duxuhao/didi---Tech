# -*- coding: utf-8 -*-
"""
Created on Sun Jun 12 11:26:12 2016

@author: 21644336
"""


import pandas as pd
import numpy as np

for d in np.arange(1, 22):
    print d
    if d < 10:
        name = '0' + str(d) + 'gap.csv'
    else:
        name = str(d) + 'gap.csv'
    frame = []
    New = pd.read_csv(name)
    New.timeslice = np.ceil(New.timeslice/5)
    for i in range(1,67):
        for t in New.timeslice.unique():      
            T = (New.district_id == i) & (New.timeslice == t)
            X = New[T]
            frame.append([i, t, d, sum(X.gap),sum(X.made)])
    
    use = pd.DataFrame(frame)
    use.columns = ['district_id', 'timeslice', 'date', 'gap', 'made']
    use.to_csv('10_'+name,index = None)
    if d == 1:
        All = use
    else:
        All = pd.concat([All,use])
        
All.to_csv('train_10_gap.csv',index = None)