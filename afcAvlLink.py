### Assign AFC rides to AVL trips
# AFC, AVL have no connecting identity in the original dataset
# So merge here on nearest time by station, route, direction
# No filtering carried out but see below for proposals


# Previously avlAfcLinkSanmay.py

#%% Imports
import pandas as pd
import numpy as np

#%%
if __name__ == '__main__':
    #%% Filenames
    avlFilename = './vars/avlActual.h5'
    ridesFilename = './vars/afcRides.csv.gz'
    stopsFilename = '../gtfs/stops.txt'
    stopStation = pd.read_csv(stopsFilename) # make sure folder name matches
    
    
    #%% Read afcRides + convert dates/times to human-readable
    afcRides = pd.read_csv(ridesFilename)#.set_index('rideID',drop=True)
    
    afcRides['hrDate'] = pd.to_datetime(afcRides['dates'],format='%Y%m%d')
    afcRides['hrInTime'] = pd.to_timedelta(afcRides['checkInTime'],unit='sec')
    afcRides['hrOutTime'] = pd.to_timedelta(afcRides['checkOutTime'],unit='sec')
    
    # Correct for DST
    cond = ((afcRides['hrDate']==pd.to_datetime(20150329,format='%Y%m%d')) & 
            ((afcRides['hrInTime']/pd.Timedelta(hours=1))>2))
    afcRides.loc[cond,'hrInTime'] = afcRides.loc[cond,'hrInTime'] - pd.Timedelta(hours=1)
    
    cond = ((afcRides['hrDate']==pd.to_datetime(20150329,format='%Y%m%d')) & 
            ((afcRides['hrOutTime']/pd.Timedelta(hours=1))>2))
    afcRides.loc[cond,'hrOutTime'] = afcRides.loc[cond,'hrOutTime'] - pd.Timedelta(hours=1)
    # Add dates to the times
    afcRides['hrDate'] = afcRides['hrDate'].dt.tz_localize('CET') # add localization
    afcRides['hrInTime'] = afcRides['hrDate'] + afcRides['hrInTime']
    afcRides['hrOutTime'] = afcRides['hrDate'] + afcRides['hrOutTime']
    
    afcRides = afcRides[['rideId',
                         'checkInStation','checkOutStation',
                         'route','dirInitial',
                         'checkInTime','hrInTime', 'hrOutTime']]
    #afcRides = afcRides.reset_index()
    #afcRides = afcRides.rename(columns={'index':'rideId'})
    
    #%% Read avl
    avl = pd.read_hdf(avlFilename)
    avl = avl[['tripId','parent_station','pointorder','actRoute','direction_id','actual','planned']]
    avl = avl.loc[~avl['parent_station'].isna()]
    avl['parent_station'] = avl['parent_station'].astype(np.int64) # type must be exactly same for merge 'by'
    avl['actRoute'] = avl['actRoute'].astype(np.int64)
    
    #%% Merge
    mergeByColsR = ['parent_station','actRoute','direction_id']
    avl = avl.sort_values('actual')
    
    mergeByColsL = ['checkInStation','route','dirInitial']
    afcRides = afcRides.sort_values('hrInTime')
    afcAvlMerge = pd.merge_asof(afcRides,avl,
                                right_on='actual',left_on='hrInTime',direction='nearest',
                                right_by=mergeByColsR,left_by=mergeByColsL)
    afcAvlMerge['diffIn'] = (afcAvlMerge['actual']-afcAvlMerge['hrInTime'])/pd.Timedelta(seconds=1)
    afcAvlMerge = afcAvlMerge[['rideId','tripId',
                               'checkInStation','pointorder','checkOutStation','route','dirInitial',
                               'hrInTime','actual','diffIn','planned','hrOutTime']]
    
    #%% Store
    storeFilename = './vars/afcAvlRides.csv.gz' #(previously afcAvlRidesSanmay.csv.gz)
#    afcAvlMerge.to_csv(storeFilename,compression='gzip',index=False) 
    sdfsdf
    #%% Filtering proposals/analysis
    condAvlCorrectMatch = afcAvlMerge['diffIn'].abs()<=120 #difference should be less than 2 minutes
    # because:
    # 1. day 1 afc information available before 1st avl information
    # 2. starting station people board early - see cond first
    # 3. line 26 has a circular route - solved
    # 4. there's a problem on 6 March 2015:
    #    a. no data for line 16 after 1800h until 0700h 7 March 2015
    #    b. no data for line 6 until 1100h 6/3/15
    #    c. journey numbers repeating for at least lines 6,16,17
    # 5. 
    condFirst = (afcAvlMerge['pointorder']==1)&((afcAvlMerge['diffIn']>=0)&(afcAvlMerge['diffIn']<=600))
    
    condAvlMatchNotFound = ~(afcAvlMerge['actual'].isna()) #actual time not found
    # because: 
    # 1. afc wrong side station (e.g., 206 Kurhaus -> 2832 Centrum);-solved
    # 2. circular route with traveller alighting one direction and sitting through direction change (line 15, 16) -solved
    # 3. direction not assigned in afc rides -solved
    # 4. ?
    
