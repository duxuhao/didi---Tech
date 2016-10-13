# -*- coding: utf-8 -*-
"""
Created on Sun May 22 15:19:11 2016

@author: 21355188
"""
# phase: data wrangling
# read order_data and convert the data into a dataframe then export into a csv file
import pandas as pd

filename = 'order_data_2016-01-01'

def read_order_data_by_date(filename):
    order_id = []
    driver_id = []
    passenger_id = []
    start_district_hash = []
    dest_district_hash = []
    price = []
    date = []
    time_of_a_day = []
    f = open(filename,'r')
    for line in f:
        content = line.split()
        order_id.append(content[0])
        driver_id.append(content[1])
        passenger_id.append(content[2])
        start_district_hash.append(content[3])
        dest_district_hash.append(content[4])
        price.append(content[5])
        date.append(content[6])
        time_of_a_day.append(content[7])
    f.close()
        

    temp = {'order_id':order_id,
            'driver_id':driver_id,
            'passenger_id':passenger_id,
            'start_district_hash':start_district_hash,
            'dest_district_hash':dest_district_hash,
            'price':price,
            'date':date,
            'time_of_a_day':time_of_a_day}
    # build the dataframe        
    temp_df = pd.DataFrame(temp)
    # export to the a csv file
    return temp_df

dates = range(1,22)
dates = [str(x) if len(str(x)) == 2 else '0'+str(x) for x in dates]
# ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21']

for n in range(21):
    print '|----------Start reading order_data_2016-01-{} -----------|'.format(dates[n])
    day = dates[n]
    filename = 'order_data_2016-01-'+day
    tdf = read_order_data_by_date(filename)
    tdf.to_csv(filename+'.csv',index = False )
    print '|------Complete exporting order_data_2016-01-{}.csv ------|'.format(dates[n])
    print '\n'
    
    