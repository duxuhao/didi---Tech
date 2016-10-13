# -*- coding: utf-8 -*-
"""
Created on Fri May 20 13:37:25 2016

@author: 21644336
"""

from collections import defaultdict
import pandas as pd
from multiprocessing import Pool
import numpy as np

pool = Pool(4)
#oder
frame = []
order_number = defaultdict(lambda: defaultdict(int))
miss = 0
made = 1
coming = 2
datelist = range(1,22)

T = []
f = open('training_data/cluster_map/cluster_map','r')
while 1:
    a = f.readline()
    if a == '':
        break
    T.append(a.strip().split('\t'))



f.close()
Cmap = pd.DataFrame(T)
Cmap.columns = ['location_ID','location_index']


for i in datelist:
    for l in Cmap.location_ID:
	for t in range(1,721):
	    order_number[(l, t, i)][made] = 0
	    order_number[(l, t, i)][miss] = 0
	    order_number[(l, t, i)][coming] = 0

for i in datelist:
    print 'date ' + str(i)
    T = []
    filename = 'training_data/order_data/order_data_2016-01-' +str(i)
    if i < 10:
        filename = 'training_data/order_data/order_data_2016-01-0' +str(i)   
    f = open(filename,'r')
    
    while 1:
        a = f.readline()
        if a == '':
            break
        new = a.strip().split('\t')
        timeslice = (int(new[-1][-8:-6]) * 60 + int(new[-1][-5:-3]))/2 + 1
        new.append(i)
        new.append((i+11) % 7)
        new.append(timeslice)
        T.append(new[1:])
        location_ID = new[3]
        driver_ID = new[1]
	passenger_ID = new[2]
	destination_ID = new[4]
	price = float(new[5])
	checkdouble = []
        if location_ID != '' and timeslice != '' and i != '':
            order_number[(location_ID, timeslice, i)][made] += 1
	    
	    if price != 0:
		order_number[(destination_ID, timeslice, i)][coming] += 1.0/ price
            if driver_ID == 'NULL':
                order_number[(location_ID, timeslice, i)][miss] += 1
                order_number[(location_ID, timeslice+1, i)][miss] += 1
		order_number[(location_ID, timeslice+2, i)][miss] += 1
		order_number[(location_ID, timeslice+3, i)][miss] += 1
		order_number[(location_ID, timeslice+4, i)][miss] += 1
    f.close()   
    frame.append(pd.DataFrame(T))

order = pd.concat(frame)
order.columns = ['driver_ID','passenger_ID','location_ID','destination_ID','price','time','date','week','timeslice']
order = order.ix[:,order.columns != 'time']

T = []
for i in order_number.items():
    T.append([i[0][0],i[0][1],i[0][2],i[1][0],i[1][1], i[1][2]])
    
OT = pd.DataFrame(T)
OT.columns = ['location_ID','timeslice','date','miss','made','coming']
'''
T = []
f = open('training_data/cluster_map/cluster_map','r')
while 1:
    a = f.readline()
    if a == '':
        break
    T.append(a.strip().split('\t'))



f.close()
Cmap = pd.DataFrame(T)
Cmap.columns = ['location_ID','location_index']
'''
Used = pd.merge(OT, Cmap, on='location_ID', left_index = True, how = 'left')
Train = Used[['location_index','date','timeslice','miss','made','coming']]
Train['week'] = pd.Series((Train.date + 11 ) % 7, index = Train.index)

#traffic
frame = []
for i in datelist:
    T = []
    filename = 'training_data/traffic_data/traffic_data_2016-01-' +str(i)
    if i < 10:
        filename = 'training_data/traffic_data/traffic_data_2016-01-0' +str(i)   
    f = open(filename,'r')
    while 1:
        a = f.readline()
        if a == '':
            break
        new = a.strip().split('\t')
        new[1] = new[1][2:]
        new[2] = new[2][2:]
        new[3] = new[3][2:]
        new[4] = new[4][2:]
        time_slice = (int(new[-1][-8:-6]) * 60 + int(new[-1][-5:-3]))/2 + 1
        new.append(i)
        new.append(time_slice)
        T.append(new)
    f.close()   
    frame.append(pd.DataFrame(T))

traffic = pd.concat(frame)
traffic.columns = ['location_ID','traffic_1','traffic_2','traffic_3','traffic_4','time','date','timeslice']
traffic = traffic.ix[:,traffic.columns != 'time']
traffic = pd.merge(traffic, Cmap, on = 'location_ID', left_index = True, how = 'left')
Train = pd.merge(Train, traffic.ix[:,traffic.columns != 'location_ID'], on=['location_index','timeslice','date'], left_index = True, how = 'left')

#weather
frame = []
for i in datelist:
    T = []
    filename = 'training_data/weather_data/weather_data_2016-01-' +str(i)
    if i < 10:
        filename = 'training_data/weather_data/weather_data_2016-01-0' +str(i)   
    f = open(filename,'r')
    while 1:
        a = f.readline()
        if a == '':
            break
        new = a.strip().split('\t')
        new.append(i)
        time_slice = (int(new[0][-8:-6]) * 60 + int(new[0][-5:-3]))/2 + 1
        new.append(time_slice)
        T.append(new)
    f.close()   
    frame.append(pd.DataFrame(T))

weather = pd.concat(frame)
weather.columns = ['time','weather','temperature','pm25','date','timeslice']
weather = weather.ix[:,weather.columns != 'time']
weather = weather.drop_duplicates(cols=['date', 'timeslice'])
Train = pd.merge(Train, weather.ix[:,weather.columns != 'location_ID'], on=['timeslice','date'], left_index = True, how = 'left')
Train = Train[~pd.isnull(Train.location_index)]
Train.to_csv('Traindata_2_10.csv',encoding = 'utf-8', index = None)
