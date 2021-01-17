### Get initial and transfer waiting times
# Various reliability representations are defined here
# Previously attributesFromTransfers2.py; also combined getIvt.py

#%% Imports
import pandas as pd
import numpy as np
import pickle
with open('./vars/stopLines.pkl','rb') as f:# so that extract routes doesn't have to run every time
    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)

#%% Actual values
cols = ['stop1','stop2','l1','l2','actualL1Time','plausibleTT','min1ActualD']
avlTransfers = pd.read_csv('./vars/transferWtActual.csv.gz',usecols=cols)
avlTransfers = avlTransfers.loc[~avlTransfers['plausibleTT'].isna()]

avlTransfers['dt'] = pd.to_datetime(avlTransfers['actualL1Time'].str[:19])
avlTransfers['day'] = avlTransfers['dt'].dt.dayofweek
avlTransfers['hour'] = avlTransfers['dt'].dt.hour
dfd
# Actual Original WT
tgroup = avlTransfers.groupby(['stop1','stop2','l1','l2','day','hour'])
actualWtStd = tgroup['plausibleTT','min1ActualD'].std()
actualWt50 = tgroup['plausibleTT','min1ActualD'].median()
actualWt95 = tgroup['plausibleTT','min1ActualD'].agg(lambda x: np.nanpercentile(x,95)) #np percentile seems to be faster than pd's native quantile
actualWt90 = tgroup['plausibleTT','min1ActualD'].agg(lambda x: np.nanpercentile(x,90)) #np percentile seems to be faster than pd's native quantile
actualWt10 = tgroup['plausibleTT','min1ActualD'].agg(lambda x: np.nanpercentile(x,10)) #np percentile seems to be faster than pd's native quantile
sds
lambdaSkew = ((actualWt90-actualWt50))/((actualWt50-actualWt10))
lambdaVar = ((actualWt90-actualWt10))/actualWt50

rbt = (actualWt95-actualWt50)
rbt.to_hdf('./vars/rbt.h5',key='df',mode='w')
actualWtStd.to_hdf('./vars/actualStd.h5',key='df',mode='w')
actualWt50.to_hdf('./vars/actualMed.h5',key='df',mode='w')
actualWt95.to_hdf('./vars/actual95.h5',key='df',mode='w')
actualWt90.to_hdf('./vars/actual90.h5',key='df',mode='w')
actualWt10.to_hdf('./vars/actual10.h5',key='df',mode='w')
lambdaSkew.to_hdf('./vars/lambdaSkew.h5',key='df',mode='w')
lambdaVar.to_hdf('./vars/lambdaVar.h5',key='df',mode='w')

#%% Planned values
cols = ['stop1','stop2','l1','l2','planL1Time','plausibleTT','min1PlanD']
avlTransfers = pd.read_csv('./vars/transferWtPlan.csv.gz',usecols=cols)
avlTransfers = avlTransfers.loc[~avlTransfers['plausibleTT'].isna()]

avlTransfers['dt'] = pd.to_datetime(avlTransfers['planL1Time'].str[:19])
avlTransfers['day'] = avlTransfers['dt'].dt.dayofweek
avlTransfers['hour'] = avlTransfers['dt'].dt.hour

tgroup = avlTransfers.groupby(['stop1','stop2','l1','l2','day','hour'])
planWt50 = tgroup['plausibleTT','min1PlanD'].median()
#planWt95 = tgroup['plausibleTT','min1PlanD'].agg(lambda x: np.nanpercentile(x,95)) #np percentile seems to be faster than pd's native quantile
#rbt = (planWt95-planWt50)/planWt50

planWt50.to_hdf('./vars/planMed.h5',key='df',mode='w')

#%% IVT
indexStation = uniqueStations[['stIndex','parent_station']].set_index('parent_station')

# Definitions
def timeDiff(tcol): 
    #need own def to handle hours > 23
    t_hour = tcol.map(lambda x: x[:2]).astype(int)
    t_min = tcol.map(lambda x: x[3:5]).astype(int)
    t_sec = tcol.map(lambda x: x[6:8]).astype(int)
    return (t_hour.diff()*3600 + t_min.diff()*60 + t_sec.diff())

# Stop times
stopTimes = pd.read_csv('../gtfs/stop_times.txt',
                        usecols = ['trip_id','stop_sequence','stop_id','arrival_time'])

stopStation = pd.read_csv('../gtfs/stops.txt',usecols = ['stop_id','parent_station'])
stopStation = stopStation.loc[~stopStation['parent_station'].isna()]
stopStation['parent_station'] = stopStation['parent_station'].str[13:].astype(int) #convert to int code
stopTimes = pd.merge(stopTimes,stopStation,on='stop_id',how='left')


tripsFile = pd.read_csv('../gtfs/trips.txt',
                        usecols = ['route_id','direction_id','trip_id','service_id'])
stopTimes = pd.merge(stopTimes,tripsFile,on='trip_id',how='left')
stopTimes['hour'] = stopTimes['arrival_time'].str[:2].astype(int)

stopTimes = stopTimes.sort_values(['route_id','direction_id','trip_id','arrival_time'])
stopTimes['stIndex'] = indexStation.loc[stopTimes['parent_station'].values].values

stopTimes['trip_id0'] = stopTimes['trip_id'].shift()
stopTimes['st_0'] = stopTimes['stIndex'].shift()
stopTimes['ivt'] = timeDiff(stopTimes['arrival_time'])

stopTimes = stopTimes.loc[stopTimes['trip_id0']==stopTimes['trip_id']]
stopTimes['st_0'] = stopTimes['st_0'].astype(int)
stopTimes['route_id'] = stopTimes['route_id'].str.split(':').str[1].astype(int)
stopTimes['direction_id'] = stopTimes['direction_id'].astype(int)

routesFile = pd.read_csv('../gtfs/routes.txt',usecols=['route_id','route_short_name'])
routesFile['route_id'] = routesFile['route_id'].str[4:].astype(int)
routesFile = routesFile.rename(columns={'route_short_name':'actRoute'})
routesFile['actRoute'] = routesFile['actRoute'].str.replace('N','9').astype(int) # night buses as 9 something
stopTimes = pd.merge(stopTimes,routesFile,on=['route_id'],how='left')
stopTimes = pd.merge(stopTimes,uniqueLines,on=['actRoute','direction_id'],how='left')

# Get IVT List
datesFile = pd.read_csv('../gtfs/calendar_dates.txt',
                        usecols = ['service_id','date'])
datesFile['day'] = pd.to_datetime(datesFile['date'],format="%Y%m%d").dt.dayofweek
datesFile = datesFile.sort_values(['service_id','day'])

ivtDf_l = []
for day in range(7):
    servicesOnDay = datesFile.loc[datesFile['day'] == day]['service_id'].values
    selectedST = stopTimes.loc[stopTimes['service_id'].isin(servicesOnDay)]
    ivtDf_t = selectedST.groupby(['st_0','stIndex','lineIndex','hour'])['ivt'].mean().reset_index()
    ivtDf_t['day'] = day
    ivtDf_l += [ivtDf_t]
    
ivtDf = pd.concat(ivtDf_l,ignore_index=True)
ivtDf = ivtDf.rename(columns={'st_0':'stop1','stIndex':'stop2'})

# Store
ivtDf.to_hdf('./vars/ivtMean.h5',key='df',mode='w')
