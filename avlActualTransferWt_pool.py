### Store all actual transfer times between all lines for each stop, 
# or each pair of transferable stops (as dictated by max. walking distance)
#
# Parallel implementation (can use multiple cores)
# Calculates the minimum and second minimum actual transfer times
# between each arriving line at a station and each departing line at the same 
# station or a walkable station.
# 1907s (40cores) (only for pool process so excluding read/write parts)
# Previously avlSanmayTransferWT_p_act.py
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
avl = pd.read_hdf('./vars/avlActual.h5')
#avl = avl.loc[~avl['planned'].isna()]

#%% Filter AVL

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
    specIdL1 = np.array(avlS.iloc[specCombos[:,0]]['tripId'],dtype=float)
    specHrL2 = np.array(avlS.iloc[specCombos[:,1]][avltype],dtype=object)
    specIdL2 = np.array(avlS.iloc[specCombos[:,1]]['tripId'],dtype=float)
    
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
    min1ActualD = []
    min1Actual1Hr = []
    min1Actual1Id = []
    min1Actual2Hr = []
    min1Actual2Id = []

    min2ActualD = []
    min2Actual1Hr = []
    min2Actual1Id = []
    min2Actual2Hr = []
    min2Actual2Id = []

    stopListA1 = []
    stopListA2 = []
    l1ListA = []
    l2ListA = []

    walkStops = np.where(lspaceWalk[stop,:])[0]
    for stop2 in walkStops:
        lineCombos,stopCombos = getLineCombos(stop,stop2,stationLines)
        avlS_actualSorted = avl.loc[avl['stIndex'].isin([stop,stop2])].reset_index(drop=True).sort_values('actual')
        
        for lc in range(len(lineCombos)):
#            print(len(lineCombos))
#            print(lc)
            avlSIndexCombos_actual = getIndexCombos(avlS_actualSorted,lineCombos,stopCombos,lc)
            if len(avlSIndexCombos_actual)==0: #!!! Note: this excludes major disruptions where a line is closed down from the comparison between actual/planned
                continue
            
            # Keep adjacent combinations
            (specDiffA,specHrL1A,specIdL1A,
             specHrL2A,specIdL2A) = getSpecCombos(avlS_actualSorted,avlSIndexCombos_actual,
                                avltype='actual',adjacency=0)
            min1ActualD += specDiffA
            min1Actual1Hr += specHrL1A
            min1Actual1Id += specIdL1A
            min1Actual2Hr += specHrL2A
            min1Actual2Id += specIdL2A

            # Keep but-one-adjacent combinations
            (specDiffA,specHrL1A,specIdL1A,
             specHrL2A,specIdL2A) = getSpecCombos(avlS=avlS_actualSorted,avlSIndexCombos=avlSIndexCombos_actual,
                                avltype='actual',adjacency=1)
            min2ActualD += specDiffA
            min2Actual1Hr += specHrL1A
            min2Actual1Id += specIdL1A
            min2Actual2Hr += specHrL2A
            min2Actual2Id += specIdL2A
            
            stopsNLinesA = np.tile(np.array([stopCombos[lc,0],stopCombos[lc,1],lineCombos[lc,0],lineCombos[lc,1]]),(len(specDiffA),1))
            stopListA1 += list(stopsNLinesA[:,0])
            stopListA2 += list(stopsNLinesA[:,1])
            l1ListA += list(stopsNLinesA[:,2])
            l2ListA += list(stopsNLinesA[:,3])
            
    if len(stopListA1)==0:
        plausibleTTA = []
    else:
        plausibleTTA = getPlausibleTransferTime(stopListA1,stopListA2,min1ActualD,min2ActualD)
    
#    print("--- %s seconds ---" % (timefxn.time() - start_time))    
    return(min1ActualD,min1Actual1Hr,min1Actual1Id,min1Actual2Hr,min1Actual2Id,
           min2ActualD,min2Actual1Hr,min2Actual1Id,min2Actual2Hr,min2Actual2Id,
           stopListA1,stopListA2,l1ListA,l2ListA,
           plausibleTTA)

#def getTransOnDate(date):
#    avlName = './vars/avlDate/avl'+date[-2:]+'.h5'
#    avlTName = './vars/avlTDate/avlT'+date[-2:]+'.h5'
#    avlDate = pd.read_hdf(avlName)
#    avlTDate = pd.read_hdf(avlTName)
#    allAvlT = []
#    
#    (min1ActualD,min1Actual1Hr,min1Actual1Id,min1Actual2Hr,min1Actual2Id,
#         min2ActualD,min2Actual1Hr,min2Actual1Id,min2Actual2Hr,min2Actual2Id,
#         stopListA1,stopListA2,l1ListA,l2ListA,
#         plausibleTTA) = zip(*map(getTransferWtPool,range(3)))
#    
#    min1ActualD = list(chain(*min1ActualD))
#    min1Actual1Hr = list(chain(*min1Actual1Hr))
#    min1Actual1Id = list(chain(*min1Actual1Id))
#    min1Actual2Hr = list(chain(*min1Actual2Hr))
#    min1Actual2Id = list(chain(*min1Actual2Id))
#    
#    min2ActualD = list(chain(*min2ActualD))
#    min2Actual1Hr = list(chain(*min2Actual1Hr))
#    min2Actual1Id = list(chain(*min2Actual1Id))
#    min2Actual2Hr = list(chain(*min2Actual2Hr))
#    min2Actual2Id = list(chain(*min2Actual2Id))
#    
#    stopListA1 = list(chain(*stopListA1))
#    stopListA2 = list(chain(*stopListA2))
#    l1ListA = list(chain(*l1ListA))
#    l2ListA = list(chain(*l2ListA))
#    
#    colNames = ['stop1','stop2','l1','l2',
#                'actualL1Id','actualL1Time','min1ActualL2Id','min1ActualL2Time','min1ActualD',
#                'min2ActualL2Id','min2ActualL2Time','min2ActualD',
#                'plausibleTT']
#    transferWtActual = pd.DataFrame(np.column_stack(
#            [stopListA1,stopListA2,l1ListA,l2ListA,
#             min1Actual1Id,min1Actual1Hr,min1Actual2Id,min1Actual2Hr,min1ActualD,
#                                         min2Actual2Id,min2Actual2Hr,min2ActualD,
#                                         plausibleTTA]),columns=colNames)
#    transferWtActual.to_csv('./vars/transferWtActual.csv.gz',compression='gzip')
#    
#    avlT = pd.DataFrame(allJourneys)
#    afterInference.columns = ['date','numTransfers','orStIndex','destStIndex',
#           'tripId0','tripId1','tripId2','tripId3',
#           'inStIndex0','inStIndex1','inStIndex2','inStIndex3',
#           'outStIndex0','outStIndex1','outStIndex2','outStIndex3',
#           'lineIndex0','lineIndex1','lineIndex2','lineIndex3',
#           'inTime0','inTime1','inTime2','inTime3',
#           'outTime0','outTime1','outTime2','outTime3']
#    afterName = './vars/afterDate/after'+date[-2:]+'.h5'
#    afterInference.to_hdf(afterName,key='df',mode='w')
#    return(allJourneys)

#%% Get Transfer WTs
from multiprocessing import Pool

print('Pool begins')
start_time = timefxn.time()
if __name__ == '__main__':
    with Pool(40) as pool:
        (min1ActualD,min1Actual1Hr,min1Actual1Id,min1Actual2Hr,min1Actual2Id,
         min2ActualD,min2Actual1Hr,min2Actual1Id,min2Actual2Hr,min2Actual2Id,
         stopListA1,stopListA2,l1ListA,l2ListA,
         plausibleTTA) = zip(*pool.map(getTransferWtPool,range(numStations)))
        pool.close()
        pool.join()

min1ActualD = list(chain(*min1ActualD))
min1Actual1Hr = list(chain(*min1Actual1Hr))
min1Actual1Id = list(chain(*min1Actual1Id))
min1Actual2Hr = list(chain(*min1Actual2Hr))
min1Actual2Id = list(chain(*min1Actual2Id))

min2ActualD = list(chain(*min2ActualD))
min2Actual1Hr = list(chain(*min2Actual1Hr))
min2Actual1Id = list(chain(*min2Actual1Id))
min2Actual2Hr = list(chain(*min2Actual2Hr))
min2Actual2Id = list(chain(*min2Actual2Id))

stopListA1 = list(chain(*stopListA1))
stopListA2 = list(chain(*stopListA2))
l1ListA = list(chain(*l1ListA))
l2ListA = list(chain(*l2ListA))

plausibleTTA = list(chain(*plausibleTTA))

print("--- %s seconds ---" % (timefxn.time() - start_time))

#%% Store
            
colNames = ['stop1','stop2','l1','l2',
            'actualL1Id','actualL1Time','min1ActualL2Id','min1ActualL2Time','min1ActualD',
                                        'min2ActualL2Id','min2ActualL2Time','min2ActualD',
                                        'plausibleTT']
transferWtActual = pd.DataFrame(np.column_stack(
        [stopListA1,stopListA2,l1ListA,l2ListA,
         min1Actual1Id,min1Actual1Hr,min1Actual2Id,min1Actual2Hr,min1ActualD,
                                     min2Actual2Id,min2Actual2Hr,min2ActualD,
                                     plausibleTTA]),columns=colNames)
transferWtActual.to_csv('./vars/transferWtActual.csv.gz',compression='gzip')
#transferWtActual.to_hdf('./vars/transferWtActual.h5',key='df',mode='w',format='table',data_columns=True)         
        