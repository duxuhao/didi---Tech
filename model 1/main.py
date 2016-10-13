# -*- coding: utf-8 -*-

from sklearn.ensemble.forest import RandomForestRegressor
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor
from sklearn.svm import SVR
from sklearn.linear_model import LinearRegression, Lasso
import pandas as pd
import numpy as np
import datetime
import random
from multiprocessing import Pool
import warnings
import xgboost as xgb
warnings.filterwarnings("ignore", category=DeprecationWarning) 
warnings.filterwarnings("ignore", category=UserWarning)

pool = Pool(8)

def mape(X, y, estimator):
    T2 = y > 0
    mapevalue = np.mean(np.abs((y[T2] - np.round(estimator.predict(X[T2]), 0)) / len(y)))#len(y[T])))
    print str(np.round(mapevalue,4)) + '\t|'
    if ~(mapevalue > 0):
	mapevalue = 0
    return mapevalue



def train(n, Train2, T, estimator = GradientBoostingRegressor(loss='huber',random_state=1)):
    print '-------------------training------------------\n'
    #print 'train location' + str(n)
    #validation = Train2[(Train.location_index == n) & (Train2.date > 14)]
    zone = Train2.location_index != 1
    for z in [51, 8, 23, 48, 37, 7, 46, 24, 28, 14, 26, 22, 4, 27, 12, 1, 42,20]:
	zone = zone & (Train2.location_index != z)
    
    timeselect = Train2.timeslice == 1800
    for d in [46, 58, 70, 82, 94, 106, 118, 130, 142]:
	#timeselect = timeselect | (Train2.timeslice == d)
        timeselect = timeselect |  (Train2.timeslice == d*2) #|  (Train2.timeslice == d*2-1) 
    
    date = range(2,22)[:1]
    TT = Train2.date == 10000
    for d in date:
	TT = (Train2.date == d) | TT

    Trainx = Train2[(Train2.date > 1)]
    #rate = 0.27
    #X1 = Trainx[Trainx.miss == 1]
    #X2 = Trainx[random.sample(Trainx[Trainx.miss != 1].index, (1/rate - 1)*len(X1))]
    #Trainx = pd.concat([X1,X2])
    validation = Train2[~TT & (Train2.date > 1)]
    prediction = Train2[(Train2.date > 15) & timeselect]
    print 'Train date: ',
    print(np.sort(Trainx.date.unique()))
    print 'Validation date: ',
    print(np.sort(validation.date.unique()))
    print 'Prediction timeslice: ',
    print(np.sort(prediction.timeslice.unique()))    
    X_train, X_test, y_train, y_test=train_test_split(Trainx[T], Trainx.miss, test_size = 0.1, random_state = 1)
    estimator.fit(X_train, y_train)
    validation['prediction'] = estimator.predict(validation[T])
    validation.loc[pd.isnull(validation.prediction),'prediction'] = 1
    #validation[['location_index','date','timeslice','prediction']].to_csv('ensembel3.csv',index = None)
    print '     location\t\tMAPE'
    mean = 0
    for n in range(1,67):
	print '-' * 33
    	print '|\t' + str(n) + '\t|\t',
    	validationl = Train2[(Train2.location_index == n) & (Train2.date > 15) & timeselect]
    	mean += mape(validationl[T], validationl.miss, estimator)
    print '-' * 33
    print '|\ttrain\t|\t',
    mape(X_train, y_train,estimator)
    print '-' * 33
    print '|\ttest\t|\t',
    mape(X_test, y_test, estimator)
    print '-' * 33
    print '|validation\t|\t',
    mape(validation[T], validation.miss, estimator)
    print '-' * 33
    print '|prediction\t|\t',
    mape(prediction[T], prediction.miss, estimator)
    print '-' * 33
    print '|   average\t|\t' + str(np.round(mean/66.0,3)) + '\t|'
    print '\n-------------finish training-----------------\n'
    return estimator

def time_sequence(df, n, fea, t = 0, w = 0):
    print '----------------combining data---------------\n'  
    Train3 = df.copy()
    for i in range(n):
	if i == 0:
	    Train3.timeslice += 2
	else:
            Train3.timeslice += 1
        X = Train3[fea]
        newname = ['date','location_index','timeslice','week']
        for s in fea[4:]:
            newname.append(s+str(i+1))
        X.columns = newname
        if t == 1:
	    print df.shape
	    print str(i),
	    print df.columns
            T = (df.columns != 'gap') & (df.columns != 'made_new')&(df.columns != 'made') & (df.columns != 'miss') & (df.columns != 'weather') & (df.columns != 'coming') & (df.columns != 'pm25')& (df.columns != 'temperature')   
            df = pd.merge(df.ix[:,T], X, on = ['date','location_index','timeslice','week'],left_index = True, how = 'left')            
	    df = df.dropna()
        else:
            df = pd.merge(df, X, on = ['date','location_index','timeslice','week'],left_index = True, how = 'inner')
    print df.shape
    df = df.dropna()
    print 'combine',
    print df.shape
    return df

def show_importance(estimator, fea):
    print '------------Features importances --------------'
    for i, f in enumerate(fea):
        if len(f) > 7:
            print f + '\t: ' + str(estimator.feature_importances_[i])
        else:
            print f + '\t\t: ' + str(estimator.feature_importances_[i])
        
def submission(test, pre, estimator, fea, n, Fea):
    print '----------------exporting data---------------\n'
    now = datetime.datetime.now()
    filename = 'submission.csv'
    print pre[:4]
    print test[:4]
    result = pd.concat([test,pre])
    testdf = time_sequence(result[fea], n, fea, 1)
    #testdf['prediction'] = pd.Series(np.round(estimator.predict(testdf[Fea]), 0),index = testdf.index)
    testdf['prediction'] = pd.Series(estimator.predict(testdf[Fea]),index = testdf.index)
    print testdf[:4]
    submission = pd.merge(pre, testdf[['date','location_index','timeslice','prediction']], on =['date','location_index','timeslice'], how = 'left')
    print submission[:4]
    submission.loc[pd.isnull(submission.prediction),'prediction'] = 1
    #submission[['location_index','time','prediction']].to_csv(filename,encoding = 'utf-8', header = None, index = None)
    print '-----------completed data export---------------\n'
    return submission[['location_index','time','prediction']]

def main():
    gap_n = 5
    w=1
    name = 'Traindata_'+ str(gap_n) +'_10.csv'
    Train = pd.read_csv(name)
    Train = Train.ix[:, (Train.columns != 'traffic_1') & (Train.columns != 'weather') & (Train.columns != 'traffic_2') &(Train.columns != 'traffic_3')&(Train.columns != 'traffic_4')&(Train.columns != 'pm25')&(Train.columns != 'temperature')]
    delete = pd.read_csv('train_5_gap.csv')
    delete.columns = ['location_index','timeslice','date','gap','made_new']
    Train = pd.merge(Train, delete, on = ['location_index','date','timeslice'],left_index = True, how = 'left')
    name = 'Testdata_'+ str(gap_n) +'_10.csv'
    Test = pd.read_csv(name)
    Test = Test[['date','location_index','timeslice','week','miss','made']]
    delete = pd.read_csv('test_5_gap.csv')
    delete.columns = ['location_index','timeslice','date','gap','made_new']
    Test = pd.merge(Test, delete, on = ['location_index','date','timeslice'],left_index = True, how = 'left')
    Pre = pd.read_csv('test22.csv')
    Pre.timeslice = Pre.timeslice * 10.0 / gap_n
    n = (10/gap_n) * 3 - 1
    duplcate_fea = ['date','location_index','timeslice','week','gap','made_new']#,'coming','pm25','temperature','gap','made_new']#'miss','made']
    traindf = time_sequence(Train, n, duplcate_fea, w = w)

    Fea = ['location_index','timeslice','week']
    for i in range(1-w,n+1):
        for s in duplcate_fea[4:]:
            Fea.append(s+str(i))

    estimator = train(i, traindf, Fea, GradientBoostingRegressor(n_estimators=120, max_depth=12, verbose=0,learning_rate=0.013,loss='huber',random_state=1, alpha=0.958))

    show_importance(estimator, Fea)
    A = submission(Test, Pre, estimator, duplcate_fea, n, Fea)

    gap_n = 5
    name = 'Traindata_'+ str(gap_n) +'_10.csv'
    Train = pd.read_csv(name)
    Train = Train.ix[:, (Train.columns != 'traffic_1') & (Train.columns != 'weather') & (Train.columns != 'traffic_2') &(Train.columns != 'traffic_3')&(Train.columns != 'traffic_4')&(Train.columns != 'pm25')&(Train.columns != 'temperature')]
    name = 'Testdata_'+ str(gap_n) +'_10.csv'
    Test = pd.read_csv(name)
    duplcate_fea = ['date','location_index','timeslice','week','miss','made']#,'coming','pm25','temperature','gap','made_new']#'miss','made']
    traindf = time_sequence(Train, n, duplcate_fea, w = w)

    Fea = ['location_index','timeslice','week']
    for i in range(1-w,n+1):
        for s in duplcate_fea[4:]:
            Fea.append(s+str(i))

    estimator = train(i, traindf, Fea, GradientBoostingRegressor(n_estimators=70, max_depth=7, verbose=0,learning_rate=0.01,loss='huber',random_state=1, alpha=0.958))

    show_importance(estimator, Fea)
    B = pd.merge(submission(Test, Pre, estimator, duplcate_fea, n, Fea),A,on=['location_index','time'],how = 'left')
    B['prediction'] = pd.Series(B.prediction_y,index = B.index)
    zone = B.location_index != 51
    for z in [51, 8, 23, 48, 37, 7, 46, 24, 28, 14, 26, 22, 4, 27, 12, 1, 42,20][:]:
        zone = zone & (B.location_index != z)
    B.loc[zone, 'prediction'] = B[zone].prediction_x
    return  df[['location_index','time','prediction']]


main().to_csv('winterfall_smallpart.csv',header = None,index = None)
