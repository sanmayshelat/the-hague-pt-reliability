### Get AFC rides
# Original dataset contains journeys but uses a simple threshold-based transfer inference
# Here, individual rides are separated in preparation for a full transfer inference based on (Menno et al, 2017)
# Previously afcRidesSanmay.py

#%% Imports
import pandas as pd
import numpy as np
import gtfsRouteExtractFxn as gtfsRoute
sdf
#%%
if __name__ == '__main__':
    #%% Filenames
    journeysFilename = '../afc/processed_Sanmay/journeys_201503.csv'
    ridesFilename = './vars/afcRides.csv.gz' #(previously afcRidesSanmay.csv.gz)
    
    #%% Get afc rides from journeys
    afc = pd.read_csv(journeysFilename,
                      usecols=['date','numTransfers','route','checkInTime','checkOutTime',
                               'checkInStop','checkOutStop'],
                      converters={'route':eval,'checkInTime':eval,'checkOutTime':eval,
                                  'checkInStop': eval,'checkOutStop': eval})
    
    lst_cols = ['route','checkInTime','checkOutTime','checkInStop','checkOutStop'] # cols with list
    idx_cols = afc.columns.difference(lst_cols)
    
    afcRides = pd.DataFrame({col:np.concatenate(afc[col].values) for col in lst_cols})
    afcRides['dates'] = np.repeat(afc['date'],afc['numTransfers']+1).values
    afcRides['dates'] = afcRides['dates'] - 60 + 20150301
    afcRides['rideId'] = list(range(len(afcRides)))
    afcRides = afcRides.reset_index(drop=True)
    afcRides.to_csv(ridesFilename,compression='gzip',index=False)
    
    #%% Assign ride ids
    afc = pd.read_csv(journeysFilename)
    a1=np.vstack([(afc['numTransfers']+1).cumsum().shift(fill_value=0).values,
                  (afc['numTransfers']+1).cumsum().values]).T
    a=[list(range(a1[i,0],a1[i,1])) for i in range(len(a1))]
    afc['rideIds'] = a
    afc.to_csv('../afc/processed_Sanmay/journeys_201503_1.csv',index=False)
    
    
    #%% Get AFC Rides
    afcRides = pd.read_csv(ridesFilename)
    
    
    stopsFilename = '../gtfs/stops.txt'
    stopStation = pd.read_csv(stopsFilename,usecols=['stop_code','parent_station']) # make sure folder name matches
    stopStation = stopStation.loc[~np.isnan(stopStation['stop_code'])]
    stopStation['parent_station'] = stopStation['parent_station'].str[13:].astype(int) #convert to int code
    stopStation = stopStation.set_index('stop_code')
    afcRides['checkInStation'] = stopStation.loc[afcRides['checkInStop'].values].values
    afcRides['checkOutStation'] = stopStation.loc[afcRides['checkOutStop'].values].values
    
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
        afcRides.loc[(afcRides['route']==i[0])&(afcRides['checkInStation']==i[1]),'checkInStation'] = i[2]
        afcRides.loc[(afcRides['route']==i[0])&(afcRides['checkOutStation']==i[1]),'checkOutStation'] = i[2]
    
    
    #%% Assign direction to rides
    afcRides['direction'] = -1
    
    for day in range(31):
        print(day)
        date = 20150301 + day
        
        # GTFS
        routeTopos = gtfsRoute.extractRoutesFromGtfs(int(date))[2]
        routeTopos = routeTopos.reset_index()#[['stop_id','stop_sequence']].reset_index()
        
        # AFC rides
        ridesOnDate = afcRides.loc[afcRides['dates']==date]
        
        for i in np.sort(afcRides['route'].unique()):
            ridesOnLine = ridesOnDate.loc[ridesOnDate['route']==i]
            
            gtfsSet = set(routeTopos.loc[routeTopos['route_id']==i,'parent_station'])
            afcSet = set(ridesOnLine['checkInStation'])
            inAfcNotGtfs = (afcSet|gtfsSet)-gtfsSet
            
            dir0 = routeTopos.loc[(routeTopos['route_id']==i)&(routeTopos['direction_id']==0)]
            dir1 = routeTopos.loc[(routeTopos['route_id']==i)&(routeTopos['direction_id']==1)]
            
            seq0in = pd.merge(ridesOnLine,dir0,left_on='checkInStation',right_on='parent_station',how='left')['stop_sequence'].values
            seq0out = pd.merge(ridesOnLine,dir0,left_on='checkOutStation',right_on='parent_station',how='left')['stop_sequence'].values
            seq1in = pd.merge(ridesOnLine,dir1,left_on='checkInStation',right_on='parent_station',how='left')['stop_sequence'].values
            seq1out = pd.merge(ridesOnLine,dir1,left_on='checkOutStation',right_on='parent_station',how='left')['stop_sequence'].values
            
            # Where both seq-in and seq-out known assign true or false to either direction        
            dirIs0 = np.zeros(len(seq0in)).astype(bool)
            cond0 = (~np.isnan(seq0out)) & (~np.isnan(seq0in))
            dirIs0[cond0] = seq0out[cond0] > seq0in[cond0]
            
            dirIs1 = np.zeros(len(seq1in)).astype(bool)
            cond1 = (~np.isnan(seq1out)) & (~np.isnan(seq1in))
            dirIs1[cond1] = seq1out[cond1] > seq1in[cond1]
            
            # If seq-in,seq-out known for one direction but not other; the one it is known for decides direction
            cond0not1 = cond0&(~cond1)
            dirIs1[cond0not1] = ~dirIs0[cond0not1] #both seq for dir0 available but not for dir1; it is not dir0
            
            cond1not0 = cond1&(~cond0)
            dirIs0[cond1not0] = ~dirIs1[cond1not0] #both seq for dir1 available but not for dir0; it is not dir1
            
            # If: seq-in,seq-out known for one dir; that dir is false; only seq-out known for other dir; then other dir is false
            # Direction assigned is initial direction.The same vehicle may very well turn around and go in the opposite direction with traveller checking out
            cond_0InOut_1OnlyIn = cond0 & (~dirIs0) & ((~np.isnan(seq1out)) & (np.isnan(seq1in)))
            cond_1InOut_0OnlyIn = cond1 & (~dirIs1) & ((~np.isnan(seq0out)) & (np.isnan(seq0in)))
            
            dirIs0[cond_0InOut_1OnlyIn] = True
            dirIs1[cond_0InOut_1OnlyIn] = False
            
            dirIs1[cond_1InOut_0OnlyIn] = True
            dirIs0[cond_1InOut_0OnlyIn] = False
            
            # If: seq-in,seq-out not known for any dir; for whichever seq-in is known is dir
            # Direction assigned is initial direction.The same vehicle may very well turn around and go in the opposite direction with traveller checking out
            cond_noInOut_Only0In = (~cond0) & (~cond1) & (~np.isnan(seq0in)) & (np.isnan(seq1in))
            cond_noInOut_Only1In = (~cond0) & (~cond1) & (~np.isnan(seq1in)) & (np.isnan(seq0in))
            
            dirIs0[cond_noInOut_Only0In] = True
            dirIs1[cond_noInOut_Only0In] = False
            
            dirIs1[cond_noInOut_Only1In] = True
            dirIs0[cond_noInOut_Only1In] = False
            
            
            # Circular route control for Zoetermeer Centrum - assign direction for those in loop according to nearest
            if i==3:
                dir0Nodes = [8117,8119,8121,8123,8125]
                dir1Nodes = [8105,8107,8109,8111,8113,8115]
                checksInHere = ridesOnLine['checkInStation']==8103
                dirIs0[checksInHere&ridesOnLine['checkOutStation'].isin(dir0Nodes)] = True
                dirIs1[checksInHere&ridesOnLine['checkOutStation'].isin(dir0Nodes)] = False
                dirIs0[checksInHere&ridesOnLine['checkOutStation'].isin(dir1Nodes)] = False
                dirIs1[checksInHere&ridesOnLine['checkOutStation'].isin(dir1Nodes)] = True
                
                dir1Nodes = [8117,8119,8121,8123,8125]
                dir0Nodes = [8105,8107,8109,8111,8113,8115]
                checksOutHere = ridesOnLine['checkOutStation']==8103
                dirIs1[checksOutHere&ridesOnLine['checkInStation'].isin(dir1Nodes)] = True
                dirIs1[checksOutHere&ridesOnLine['checkInStation'].isin(dir0Nodes)] = False
                dirIs0[checksOutHere&ridesOnLine['checkInStation'].isin(dir1Nodes)] = False
                dirIs0[checksOutHere&ridesOnLine['checkInStation'].isin(dir0Nodes)] = True
                
    
            dirNotAvail = ~(dirIs0 | dirIs1)
            dirConfusing = dirIs0 & dirIs1
            if any(dirNotAvail) | any(dirConfusing):
                print(date,i,dirNotAvail.sum(),dirConfusing.sum())
                
            ridesOnLine.loc[dirIs0,'direction'] = 0
            ridesOnLine.loc[dirIs1,'direction'] = 1
            
            afcRides.loc[ridesOnLine.index,'direction'] = ridesOnLine['direction']
            
    #%%
    afcRides = afcRides.rename(columns={'direction':'dirInitial'}) 
    afcRides.to_csv(ridesFilename,compression='gzip',index=False)        

