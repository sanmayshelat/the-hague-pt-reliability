### Merge AVL planned, with GTFS
# AVL only has information on line number and not direction, so assign direction
# Previously called avlSanmay1.py

#%% Imports
import pandas as pd
import numpy as np
import gtfsRouteExtractFxn as gtfsRoute
import pickle
with open('./vars/stopLines.pkl','rb') as f:# so that extract routes doesn't have to run every time
    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)
    
#%% Column names
ttColNames = ['operatingday','trip_id','pointorder','passagesequencenumber',
              'userstopcode','targetarrivaltime','targetdeparturetime','trip_hash']

# Used columns
ttColUsed = ['operatingday','trip_id','pointorder','userstopcode',
             'targetarrivaltime','targetdeparturetime']


#%% GTFS Reads
datesFile = pd.read_csv('../gtfs/calendar_dates.txt')
tripsFile = pd.read_csv('../gtfs/trips.txt', 
                        usecols = ['route_id','service_id','trip_id','direction_id','trip_headsign'])
stopTimesFile = pd.read_csv('../gtfs/stop_times.txt',
                            usecols = ['trip_id','stop_sequence','stop_id','arrival_time','timepoint'])
routesFile = pd.read_csv('../gtfs/routes.txt')

#%% Definitions
def getAvlPlanned(date):
    avlTT = pd.read_csv('../avl/HTM_TT_'+str(date)+'.csv.gz',delimiter=',',header=None,
                        names=ttColNames,index_col=False,usecols=ttColUsed)
    avlTT['journeynumber'] = avlTT['trip_id'].str.split(':').str[-1].astype(int)
    avlTT['lineplanningnumber'] = avlTT['trip_id'].str.split(':').str[-2].astype(int)
    
    avlTT.drop(['trip_id'],axis=1,inplace=True)
    return(avlTT)

def directionAssignment(avl,routeTopos):
    avl['direction_id'] = -1
    avlLines = avl['lineplanningnumber'].unique()
    for i in avlLines:
#        print(i)
#        i = 1
        avl_line = avl.loc[avl['lineplanningnumber']==i]
        dir0 = routeTopos.loc[(routeTopos['route_id']==i)&(routeTopos['direction_id']==0)]
        dir1 = routeTopos.loc[(routeTopos['route_id']==i)&(routeTopos['direction_id']==1)]
        avlJourneys = avl_line['journeynumber'].unique()
        for j in avlJourneys:
#            j = avlJourneys[0]
            avl_j = avl_line.loc[avl_line['journeynumber']==j]
            
            dir0Pos0 = 999
            dir1Pos0 = 999
            dir0Pos1 = 999
            dir1Pos1 = 999            
            
            flag0 = 0
            flag1 = 0
            for k_0 in range(len(avl_j['userstopcode'].values)):
                k0 = avl_j['userstopcode'].values[k_0]
                if (flag0==0)&(any(dir0['stop_id']==k0)):
                    dir0Pos0 = np.where(dir0['stop_id'].values==k0)[0][0]
                    dir0stop0 = k0
                    flag0 = 1
                    
                if (flag1==0)&(any(dir1['stop_id']==k0)):
                    dir1Pos0 = np.where(dir1['stop_id'].values==k0)[0][0]
                    dir1stop0 = k0
                    flag1 = 1
                    
                if (flag0==1)&(flag1==1):
                    break
            
            flag0 = 0
            flag1 = 0
            for k_1 in range(len(avl_j['userstopcode'].values)):
                k1 = avl_j['userstopcode'].values[k_1]
                if (flag0==0)&(any(dir0['stop_id']==k1)):
                    if dir0Pos0==999:
                        dir0Pos1 = np.where(dir0['stop_id'].values==k1)[0][0]
                        flag0 = 1
                    elif k1 != dir0stop0:
                        dir0Pos1 = np.where(dir0['stop_id'].values==k1)[0][0]
                        flag0 = 1
                        
                    
                if (flag1==0)&(any(dir1['stop_id']==k1)):
                    if dir1Pos0==999:
                        dir1Pos1 = np.where(dir1['stop_id'].values==k1)[0][0]
                        flag1 = 1
                    elif k1 != dir1stop0:
                        dir1Pos1 = np.where(dir1['stop_id'].values==k1)[0][0]
                        flag1 = 1
                    
                if (flag0==1)&(flag1==1):
                    break
            
            if (dir0Pos0!=999) & (dir0Pos1!=999):
                if dir0Pos0<dir0Pos1:
                    avl.loc[avl['journeynumber']==j,'direction_id'] = 0
            if (dir1Pos0!=999) & (dir1Pos1!=999):
                if dir1Pos0<dir1Pos1:
                    avl.loc[avl['journeynumber']==j,'direction_id'] = 1

    return(avl)

def dateConversion1(avl,date):
    if date == 20150328: # because DST
        avl['planned'] = pd.to_timedelta(avl['targetdeparturetime'])
        t_2oclock = (avl['planned']/pd.Timedelta(hours=1) >= 26) & (avl['planned']/pd.Timedelta(hours=1) < 27)
        avl.loc[t_2oclock,'planned'] = avl.loc[t_2oclock,'planned']+pd.Timedelta(hours=1)
        avl['planned'] = (pd.to_datetime(avl['operatingday']) + avl['planned']).dt.tz_localize('CET')
        
    else:
        avl['planned'] = (pd.to_datetime(avl['operatingday']) + pd.to_timedelta(avl['targetdeparturetime'])).dt.tz_localize('CET')
        
    return(avl)

#%%
if __name__ == '__main__':
    #%% Combining AVL actual, AVL planned, GTFS
    avlStore = pd.DataFrame()
    for day in range(31):
        print(day)
        date = 20150301 + day
        
        # GTFS
        routeTopos = gtfsRoute.extractRoutesFromGtfs(int(date))[2]
        routeTopos = routeTopos.reset_index()
        # remove Zoetermeer Centrum because it is cyclic station
        routeTopos = routeTopos.loc[~((routeTopos['route_id']==3)&(routeTopos['parent_station']==8103))]
        
        # Planned Times
        avlTT = getAvlPlanned(date)
        
        # Assign direction
        avlTT.drop_duplicates(['journeynumber','userstopcode'],keep='last',inplace=True) #only keep final departure from stop
        avlTT = avlTT.sort_values(['targetdeparturetime','journeynumber'])
        avlTT = directionAssignment(avlTT,routeTopos)    
        print((avlTT['direction_id']==-1).sum())
        avlTT = avlTT.loc[avlTT['direction_id']!=-1] # remove journeys for which direction couldn't be assigned
        
        # Convert dates
        avlTT = dateConversion1(avlTT,date)
        
        # Final arrangements
        avlTT.drop(['operatingday'],axis=1,inplace=True)
        
        # Rename columns
        avlTT = avlTT.rename(columns={'lineplanningnumber':'route_id','userstopcode':'stop_id'})
        
        avlStore = avlStore.append(avlTT,ignore_index=True)
    
    #%% AVL stops and lines
    routesFile = pd.read_csv('../gtfs/routes.txt',usecols=['route_id','route_short_name'])
    routesFile['route_id'] = routesFile['route_id'].str[4:].astype(int)
    routesFile = routesFile.rename(columns={'route_short_name':'actRoute'})
    routesFile['actRoute'] = routesFile['actRoute'].str.replace('N','9').astype(int) # night buses as 9 something
    avlStore = pd.merge(avlStore,routesFile,on=['route_id'],how='left')
    
    avlStore = pd.merge(avlStore,uniqueLines,on=['actRoute','direction_id'],how='left')
    
    
    #Stop-Station-Index map
    stopsFile = '../gtfs/stops.txt'
    stopStation = pd.read_csv(stopsFile,usecols=['stop_code','parent_station']) # make sure folder name matches
    stopStation = stopStation.loc[~np.isnan(stopStation['stop_code'])]
    stopStation['parent_station'] = stopStation['parent_station'].str[13:].astype(int) #convert to int code
    stopStation = stopStation.rename(columns={'stop_code':'stop_id'})
    avlStore = pd.merge(avlStore,stopStation,on=['stop_id'],how='left')
    
    avlStore = pd.merge(avlStore,uniqueStations,on=['parent_station'],how='left')
    
    
    avlStore.to_hdf('./vars/avlPlanned.h5',key='df',mode='w') #(previously stored as avlPlanSanmay.h5)
    
