import pandas as pd
import numpy as np

df1 = pd.read_csv('result/model11.csv',header = None)
df2 = pd.read_csv('result/model22.csv',header = None)
df1.columns = ['location_index','time','prediction']
df2.columns = ['location_index','time','prediction']
df =pd.merge(df1,df2,on = ['location_index','time'],how = 'left')
df['prediction'] = pd.Series(np.round(0.7 * df.prediction_y + 0.3 * df.prediction_x,0),index = None)
df[df.prediction < 1] = 1
df[['location_index','time','prediction']].to_csv('result/final.csv', header = None, index = None)