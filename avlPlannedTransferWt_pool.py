### Store all planned transfer times between all lines for each stop, 
# or each pair of transferable stops (as dictated by max. walking distance)
#
# Parallel implementation (can use multiple cores)
# Calculates the minimum and second minimum planned and actual transfer times
# between each arriving line at a station and each departing line at the same 
# station or a walkable station.
# 1744s (40cores) (only for pool process so excluding read/write parts)
# Previously avlSanmayTransferWT_p_plan.py
#%% Imports
import pandas as pd
import time as timefxn
import numpy as np
from itertools import product, chain
start_time  = timefxn.time()
import pickle
with open('./vars/stopLines.pkl','rb') as f:# so that extract routes doesn't have to run every time
    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)

#%% Load AVL
avl = pd.read_hdf('./vars/avlPlanned.h5')

#%% Filter AVL
#dayCond = avl['estDeparture'].dt.dayofweek<5 # no weekends
#hrCond = (avl['estDeparture'].dt.hour>4) & (avl['estDeparture'].dt.hour<13)
#cond = dayCond & hrCond
#
#avl = avl.loc[cond]
# stationLines,allStations,lspacewalk
#%% Definitions
# Get stop/line combinations for which transfer wts are to be calculated
def getLineCombos(stop,stop2,stationLines):
    lineCxns1 = np.where(stationLines[stop,:])[0]
    lineCxns2 = np.where(stationLines[stop2,:])[0]
    lineCombos = np.array(list(product(lineCxns1,lineCxns2)))
    stopCombos = np.tile(np.array([stop,stop2]),(np.shape(lineCombos)[0],1))
    return(lineCombos,stopCombos)

# Get combos of indices of trips of lines in lc
def getIndexCombos(avlS,lineCombos,stopCombos,lc):
    cond1 = (avlS['lineIndex']==lineCombos[lc,0]) & (avlS['stIndex']==stopCombos[lc,0])
    cond2 = (avlS['lineIndex']==lineCombos[lc,1]) & (avlS['stIndex']==stopCombos[lc,1])
    
    ic1 = np.where(cond1)[0]
    ic2 = np.where(cond2)[0]
    
    next2Trips = np.array([ic2[ic2>i][:2] for i in ic1 if len(ic2[ic2>i][:2])==2]) # removes trips where next two trips are not available; typically at end of dataset?
    if len(next2Trips)==0:
        return([])
    haveNext2Trips = np.array([len(ic2[ic2>i][:2])==2 for i in ic1]) # removes trips where next two trips are not available; typically at end of dataset?
    avlSIndexCombos = np.hstack([ic1[haveNext2Trips,None],next2Trips])
    
    return(avlSIndexCombos)
    
# Get specific combinations
def getSpecCombos(avlS,avlSIndexCombos,avltype,adjacency):
    # avltype = 'actual' or 'planned'
    # adjacency is number of trips of l2 after l1, 0th trip is the first
    
    specCombos = avlSIndexCombos[:,(0,adjacency+1)]
    specDiff = np.array((avlS.iloc[specCombos[:,1]][avltype].reset_index(drop=True)-\
                         avlS.iloc[specCombos[:,0]][avltype].reset_index(drop=True)).dt.total_seconds())
    specHrL1 = np.array(avlS.iloc[specCombos[:,0]][avltype],dtype=object)
    specIdL1 = np.array(avlS.iloc[specCombos[:,0]]['journeynumber'],dtype=float)
    specHrL2 = np.array(avlS.iloc[specCombos[:,1]][avltype],dtype=object)
    specIdL2 = np.array(avlS.iloc[specCombos[:,1]]['journeynumber'],dtype=float)
    
    cond2 = np.array(avlS.iloc[specCombos[:,0]][avltype].dt.date.values!=avlS.iloc[specCombos[:,1]][avltype].dt.date.values) # remove combos across dates; #!!! will cause issues for late-night trips
    specDiff[cond2] = np.nan
    specHrL1[cond2] = np.nan
    specIdL1[cond2] = np.nan
    specHrL2[cond2] = np.nan
    specIdL2[cond2] = np.nan
    
    return(list(specDiff),list(specHrL1),list(specIdL1),list(specHrL2),list(specIdL2))
    
def getPlausibleTransferTime(stopList1,stopList2,min1D,min2D):
    stop = np.array(stopList1)
    stop2 = np.array(stopList2)
    diff = np.array(min1D)
    diff2 = np.array(min2D)
    
    timeThreshold = stationWalkingTimes[stop,stop2]
    cond_notnan = ~np.isnan(diff)
    cond = np.less(diff,timeThreshold,where=~np.isnan(diff)) #ignores the ones with nan but leaves them as true
#    (diff[cond_notnan]<timeThreshold[cond_notnan]) #non nan spec Diff is less than time threshold
    
    plausibleTT = diff
    plausibleTT[cond_notnan & cond] = diff2[cond_notnan & cond]
    return(list(plausibleTT))

# Main function as definition to allow multiprocessing
def getTransferWtPool(stop):
#    start_time = timefxn.time()
    min1PlanD = []
    min1Plan1Hr = []
    min1Plan1Id = []
    min1Plan2Hr = []
    min1Plan2Id = []
    
    min2PlanD = []
    min2Plan1Hr = []
    min2Plan1Id = []
    min2Plan2Hr = []
    min2Plan2Id = []

    stopListP1 = []
    stopListP2 = []
    l1ListP = []
    l2ListP = []
    
    
    walkStops = np.where(lspaceWalk[stop,:])[0]
    for stop2 in walkStops:
        lineCombos,stopCombos = getLineCombos(stop,stop2,stationLines)
        avlS_planSorted = avl.loc[avl['stIndex'].isin([stop,stop2])].reset_index(drop=True).sort_values('planned')
        
        for lc in range(len(lineCombos)):
#            print(len(lineCombos))
#            print(lc)
            avlSIndexCombos_plan = getIndexCombos(avlS_planSorted,lineCombos,stopCombos,lc)
            if len(avlSIndexCombos_plan)==0: #!!! Note: this excludes major disruptions where a line is closed down from the comparison between actual/planned
                continue
            
            # Keep adjacent combinations
            (specDiffP,specHrL1P,specIdL1P,
             specHrL2P,specIdL2P) = getSpecCombos(avlS_planSorted,avlSIndexCombos_plan,
                                avltype='planned',adjacency=0)
            min1PlanD += specDiffP
            min1Plan1Hr += specHrL1P
            min1Plan1Id += specIdL1P
            min1Plan2Hr += specHrL2P
            min1Plan2Id += specIdL2P
            
            # Keep but-one-adjacent combinations
            (specDiffP,specHrL1P,specIdL1P,
             specHrL2P,specIdL2P) = getSpecCombos(avlS_planSorted,avlSIndexCombos_plan,
                                avltype='planned',adjacency=1)
            min2PlanD += specDiffP
            min2Plan1Hr += specHrL1P
            min2Plan1Id += specIdL1P
            min2Plan2Hr += specHrL2P
            min2Plan2Id += specIdL2P
            
            stopsNLinesP = np.tile(np.array([stopCombos[lc,0],stopCombos[lc,1],lineCombos[lc,0],lineCombos[lc,1]]),(len(specDiffP),1))
            stopListP1 += list(stopsNLinesP[:,0])
            stopListP2 += list(stopsNLinesP[:,1])
            l1ListP += list(stopsNLinesP[:,2])
            l2ListP += list(stopsNLinesP[:,3])
            
    if len(stopListP1)==0:
        plausibleTTP = []
    else:
        plausibleTTP = getPlausibleTransferTime(stopListP1,stopListP2,min1PlanD,min2PlanD)
#    print("--- %s seconds ---" % (timefxn.time() - start_time))    
    return(min1PlanD,min1Plan1Hr,min1Plan1Id,min1Plan2Hr,min1Plan2Id,
           min2PlanD,min2Plan1Hr,min2Plan1Id,min2Plan2Hr,min2Plan2Id,
           stopListP1,stopListP2,l1ListP,l2ListP,
           plausibleTTP)

#%% Get Transfer WTs
from multiprocessing import Pool

print('Pool begins')
start_time = timefxn.time()
if __name__ == '__main__':
    with Pool(40) as pool:
        (min1PlanD,min1Plan1Hr,min1Plan1Id,min1Plan2Hr,min1Plan2Id,
         min2PlanD,min2Plan1Hr,min2Plan1Id,min2Plan2Hr,min2Plan2Id,
         stopListP1,stopListP2,l1ListP,l2ListP,
         plausibleTTP) = zip(*pool.map(getTransferWtPool,range(numStations)))
        pool.close()
        pool.join()

min1PlanD = list(chain(*min1PlanD))
min1Plan1Hr = list(chain(*min1Plan1Hr))
min1Plan1Id = list(chain(*min1Plan1Id))
min1Plan2Hr = list(chain(*min1Plan2Hr))
min1Plan2Id = list(chain(*min1Plan2Id))

min2PlanD = list(chain(*min2PlanD))
min2Plan1Hr = list(chain(*min2Plan1Hr))
min2Plan1Id = list(chain(*min2Plan1Id))
min2Plan2Hr = list(chain(*min2Plan2Hr))
min2Plan2Id = list(chain(*min2Plan2Id))

stopListP1 = list(chain(*stopListP1))
stopListP2 = list(chain(*stopListP2))
l1ListP = list(chain(*l1ListP))
l2ListP = list(chain(*l2ListP))

plausibleTTP = list(chain(*plausibleTTP))

print("--- %s seconds ---" % (timefxn.time() - start_time))

#%% Store
colNames = ['stop1','stop2','l1','l2',
            'planL1Id','planL1Time','min1PlanL2Id','min1PlanL2Time','min1PlanD',
                                    'min2PlanL2Id','min2PlanL2Time','min2PlanD',
                                    'plausibleTT']
transferWtPlan = pd.DataFrame(np.column_stack(
        [stopListP1,stopListP2,l1ListP,l2ListP,
         min1Plan1Id,min1Plan1Hr,min1Plan2Id,min1Plan2Hr,min1PlanD,
                                 min2Plan2Id,min2Plan2Hr,min2PlanD,
                                 plausibleTTP]),columns=colNames)
transferWtPlan.to_csv('./vars/transferWtPlan.csv.gz',compression='gzip')
