#%% Imports
import pandas as pd
import numpy as np
import pickle
with open('./vars/stopLines.pkl','rb') as f:# so that extract routes doesn't have to run every time
    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)

if __name__ == '__main__':
    #%% File names
    journeysFilename = '../afc/processed_Sanmay/journeys_201503_1.csv'
    ridesFilename = './vars/afcAvlRides.csv.gz'
    
    #%% Station-Index map
    indexStation = uniqueStations[['stIndex','parent_station']].set_index('parent_station')
    indexLines = uniqueLines.set_index(['actRoute','direction_id'])
    
    #%% Read afcRides + convert dates/times to human-readable
    afcRides = pd.read_csv(ridesFilename)#.set_index('rideID',drop=True)
    afcRides = afcRides.sort_values('rideId')
    #!!! Filter out rides which could not be assigned a trip id (generally because they couldn't be assigned a direction)
    voidRides = afcRides.loc[afcRides['tripId'].isna(),'rideId']
    
    afcRides['hrInTime'] = pd.to_datetime(afcRides['hrInTime'].str[:-6]).dt.tz_localize('CET')
    afcRides['hrOutTime'] = pd.to_datetime(afcRides['hrOutTime'].str[:-6]).dt.tz_localize('CET')
    
    
    #%% Create combined file: as separate columns
    #!!! Dates lose localization because of .values
    b = pd.read_csv(journeysFilename,usecols=['date','numTransfers','rideIds','route',
                                              'origin','dest'],converters={'rideIds': eval,'route':eval})
    
    b['firstRoute'] = [i[0] for i in b['route']]
    b['lastRoute'] = [i[-1] for i in b['route']]
    
    # Manual correction of wrong nodes
    wrongNodes = np.array([[2,2698,2658],
                           [3,2821,2823],
                           [4,2821,2823],
                           [4,2936,2903],
                           [21,3215,3227],
                           [22,801,802],
                           [23,3215,3217],
                           [23,5411,5421],
                           [26,3215,3217]])
    for i in wrongNodes:
        b.loc[(b['firstRoute']==i[0])&(b['origin']==i[1]),'origin'] = i[2]
        b.loc[(b['lastRoute']==i[0])&(b['dest']==i[1]),'dest'] = i[2]
    
    b['orStIndex'] = indexStation.loc[b['origin'].values].values
    b['destStIndex'] = indexStation.loc[b['dest'].values].values
    
    
    for i in range((b['numTransfers']+1).max()):
        condT = (b['numTransfers']+1)>i
        
        b.loc[condT,'rideId'+str(i)] = np.array([j[i] for j in b.loc[condT,'rideIds']]).astype(int)
        
        condNotVoid = (~b['rideId'+str(i)].isin(voidRides.values))
        
        cond = condT & condNotVoid
        bTemp = afcRides['rideId'].isin(b.loc[cond,'rideId'+str(i)].values)
        
        
        b.loc[cond,'tripId'+str(i)] = afcRides.loc[bTemp,'tripId'].values
        
        b.loc[cond,'lineId'+str(i)] = afcRides.loc[bTemp,'route'].values
        b.loc[cond,'dirId'+str(i)] = afcRides.loc[bTemp,'dirInitial'].values
        b.loc[cond,'lineIndex'+str(i)] = pd.merge(b.loc[cond],uniqueLines,how='left',
              left_on=['lineId'+str(i),'dirId'+str(i)],right_on=['actRoute','direction_id'])['lineIndex'].values
        
        b.loc[cond,'inStation'+str(i)] = afcRides.loc[bTemp,'checkInStation'].values
        b.loc[cond,'outStation'+str(i)] = afcRides.loc[bTemp,'checkOutStation'].values
        b.loc[cond,'inStIndex'+str(i)] = indexStation.loc[b.loc[cond,'inStation'+str(i)].values,'stIndex'].values
        b.loc[cond,'outStIndex'+str(i)] = indexStation.loc[b.loc[cond,'outStation'+str(i)].values,'stIndex'].values
        
        b.loc[cond,'inTime'+str(i)] = np.array(afcRides.loc[bTemp]['hrInTime'],dtype=object)
        b.loc[cond,'outTime'+str(i)] = np.array(afcRides.loc[bTemp,'hrOutTime'],dtype=object)
    
    
    notVoidJourneys = np.ones(len(b)).astype(bool)
    for i in range((b['numTransfers']+1).max()):
        notVoidJourneys = notVoidJourneys & (~b['rideId'+str(i)].isin(voidRides.values))
    
    b = b.loc[notVoidJourneys]
    
    b = b[['date',
           'numTransfers','orStIndex','destStIndex',
           'tripId0','tripId1','tripId2','tripId3',
           'inStIndex0','inStIndex1','inStIndex2','inStIndex3',
           'outStIndex0','outStIndex1','outStIndex2','outStIndex3',
           'lineIndex0','lineIndex1','lineIndex2','lineIndex3',
           'inTime0','inTime1','inTime2','inTime3',
           'outTime0','outTime1','outTime2','outTime3']]
    
    #%% Store
    b.to_hdf('./vars/afcRestructured.h5',key='df',mode='w') #(previously afcSanmayRestructuredC.h5)
    b.loc[b['numTransfers']>0].to_hdf('./vars/afcTripsWithTransfers.h5',key='df',mode='w') #(previously afcTransfers.h5)









