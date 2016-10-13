# -*- coding: utf-8 -*-
"""
Created on Mon May 23 16:38:41 2016

@author: 21355188
"""

# phase: data wrangling
# read the weather data to export a csv file
import pandas as pd

def read_weather_data_by_date(filename):
    f = open(filename,'r')
    date = []
    time_of_a_day = []
    weather = []
    temperature = []
    pm25 = []
    for line in f:
        contents = line.split()
        date.append(contents[0])
        time_of_a_day.append(contents[1])
        weather.append(contents[2])
        temperature.append(contents[3])
        pm25.append(contents[4])
    
    f.close()
    tdict = {'date':date,
             'time_of_a_day':time_of_a_day,
             'weather':weather,
             'temperature':temperature,
             'pm25':pm25}
    tdf = pd.DataFrame(tdict)
    return tdf
    
dates = range(1,22)
dates = [str(x) if len(str(x)) == 2 else '0'+str(x) for x in dates]
# ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21']

for n in range(21):
    print '|----------Start reading weather_data_2016-01-{} -----------|'.format(dates[n])
    day = dates[n]
    filename = 'weather_data_2016-01-'+day
    tdf = read_weather_data_by_date(filename)
    tdf.to_csv(filename+'.csv',index = False )
    print '|------Complete exporting weather_data_2016-01-{}.csv ------|'.format(dates[n])
    print '\n'


