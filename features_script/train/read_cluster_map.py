# -*- coding: utf-8 -*-
"""
Created on Mon May 23 16:24:11 2016

@author: 21355188
"""

# phase: data wrangling
# read the clustermap to export a csv file
import pandas as pd



filename = 'cluster_map'
f = open(filename,'r')
district_hash = []
district_id = []
for line in f:
    contents = line.split()
    district_hash.append(contents[0])
    district_id.append(contents[1])
    
f.close()

tdict = {'district_hash':district_hash,
         'district_id':district_id}
         
         
tdf = pd.DataFrame(tdict)
tdf.to_csv(filename+'.csv',index = False)
