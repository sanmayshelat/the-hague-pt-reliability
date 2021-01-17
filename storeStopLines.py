### Store information on stops and lines
# Walking speed and buffer can be changed here
# Previously stopLineDeets.py

#%% Imports
import pandas as pd
import numpy as np
import myGlobalFxns as gf

#%%
if __name__ == '__main__':
    avlStore = pd.read_hdf('./vars/avlActual.h5') #(previously stored as avlSanmay.h5)
    
    uniqueLines = avlStore[['actRoute','direction_id']].drop_duplicates()
    uniqueLines = uniqueLines.sort_values(['actRoute','direction_id']).reset_index(drop=True).reset_index()
    uniqueLines = uniqueLines.rename(columns={'index':'lineIndex'})
    numLines = len(uniqueLines)
    
    # Station information
    stopsFile = '../gtfs/stops.txt'
    stopStation = pd.read_csv(stopsFile,usecols=['stop_code','parent_station','stop_lat', 'stop_lon','stop_name']) # make sure folder name matches
    stopStation = stopStation.loc[~np.isnan(stopStation['stop_code'])]
    stopStation['parent_station'] = stopStation['parent_station'].str[13:].astype(int) #convert to int code
    t = stopStation.groupby('parent_station')
    stopStation['stationLat'] = t['stop_lat'].transform('mean')
    stopStation['stationLon'] = t['stop_lon'].transform('mean')
    stopStation = stopStation.rename(columns={'stop_code':'stop_id'})
    
    uniqueStations = stopStation.drop(['stop_id','stop_lat','stop_lon'],axis=1).drop_duplicates()
    uniqueStations = uniqueStations.reset_index(drop=True).reset_index()
    uniqueStations = uniqueStations.rename(columns={'index':'stIndex'})
    numStations = len(uniqueStations)
    
    
    # Walkable transfers
    stationDists = gf.latLonDistArray(uniqueStations['stationLat'].values,
                                      uniqueStations['stationLon'].values)
    stationDists = stationDists
    walkingSpeed = 0.66 #m/s from Menno
    stationWalkingTimes = stationDists/walkingSpeed
    pWalkableTransferDist = 400 # metres #changed to 400 meters on 1/6/2019
    
    t_walkable = np.argwhere(stationDists<pWalkableTransferDist)
    lspaceWalk = np.zeros((numStations,numStations))
    for i in np.unique(t_walkable):
        lspaceWalk[i,t_walkable[t_walkable[:,0]==i,1]] = 1
    lspaceWalk = lspaceWalk.astype(bool)
    
    # Station-Lines
    stationLinesPd = avlStore[['lineIndex','stIndex']].drop_duplicates().groupby('stIndex')['lineIndex'].unique()
    stationLines = np.zeros((numStations,numLines),dtype=int)
    for i in range(numStations):
        if any(stationLinesPd.index==i):
            for j in stationLinesPd.loc[i].astype(int):
                stationLines[i,j] = 1
    stationLines = stationLines.astype(bool)
    
    import pickle
    filename = './vars/stopLines.pkl' #(previously stored as stopLineDeets.pkl)
    with open(filename,'wb') as f:
        pickle.dump([uniqueStations,numStations,uniqueLines,numLines,stationLines,
                     lspaceWalk,stationDists,stationWalkingTimes],f)
