### Transfer inference
# Previously afcTopoGtfsMerge2.py

# 6430s (40 cores) with saving result of each pool


#%% Imports
import pandas as pd
import numpy as np
from multiprocessing import Pool
from itertools import chain
import time as timefxn
#import pickle
#with open('./vars/stopLineDeets.pkl','rb') as f:# so that extract routes doesn't have to run every time
#    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
#     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)
#zx
#%% Definitions
def getTransferTimes(avlTDate,tripIds,inSt,outSt,lines,j):
    cond1 = ((avlTDate['actualL1Id']==tripIds[j])&
             (avlTDate['stop1']==outSt[j])&
             (avlTDate['stop2']==inSt[j+1])&
             (avlTDate['l2']==lines[j+1]))
    cond2 = ((avlTDate['actualL1Id']==tripIds[j+1])&
             (avlTDate['stop1']==inSt[j+1]))
    
    if (cond1.sum()==0)|(cond2.sum()==0): #trips near the end of day (midnight) might get removed
        actualTT = []
        plausibleTT = []
    elif (cond1.sum()>1):
        plausibleTT = avlTDate.loc[cond1]['plausibleTT'].values
        times1 = avlTDate.loc[cond1]['actualL1Time'].sort_values().values
        times2 = avlTDate.loc[cond2]['actualL1Time'].drop_duplicates().sort_values().values
        actualTT = ((times2-times1[:,np.newaxis])/np.timedelta64(1,'s')).flatten()
        tempPos1 = (np.abs(actualTT-plausibleTT[:,np.newaxis]).argmin())%(len(actualTT))
        tempPos2 = (np.abs(actualTT-plausibleTT[:,np.newaxis]).argmin())/(len(actualTT))
        actualTT = np.array([actualTT[tempPos1]])
        plausibleTT = np.array([plausibleTT[int(tempPos2)]])
    else:
        plausibleTT = avlTDate.loc[cond1]['plausibleTT'].values
    #    actualTT = (avlTDate.loc[cond2]['dt'].values[0]-
    #                avlTDate.loc[cond1]['dt']).dt.total_seconds().values
        times1 = avlTDate.loc[cond1]['actualL1Time'].sort_values().values
        times2 = avlTDate.loc[cond2]['actualL1Time'].drop_duplicates().sort_values().values
        actualTT = (times2-times1)/np.timedelta64(1,'s')
        if len(actualTT)>1:
            actualTT = np.array([actualTT[np.abs(actualTT-plausibleTT).argmin()]])
        
    return(actualTT,plausibleTT)

def createNewJourneys(separateLegs,removeLegs,journey,
                      inSt,outSt,lines,tripIds):
    validLegs = list(set(range(journey['numTransfers']+1))-set(removeLegs))
    newJourneys = np.split(validLegs,np.searchsorted(validLegs,separateLegs)+1)
    
    inTime = np.array([journey['inTime'+str(k)] for k in range(journey['numTransfers']+1)])
    outTime = np.array([journey['outTime'+str(k)] for k in range(journey['numTransfers']+1)])
    
    journeysList = []
    for i in newJourneys:
        if len(i)==0:
            continue
        numLegs = len(i)
        jList = np.zeros(len(journey))
        jList[:] = np.nan
        jList = jList.tolist()
        
        jList[1] = numLegs-1
        jList[2:4] = [inSt[i[0]],outSt[i[-1]]]
        jList[4:4+numLegs] = tripIds[i]
        jList[8:8+numLegs] = inSt[i]
        jList[12:12+numLegs] = outSt[i]
        jList[16:16+numLegs] = lines[i]
        jList[20:20+numLegs] = inTime[i]
        jList[24:24+numLegs] = outTime[i]
        jList[0] = journey['date']
        journeysList += [jList]
        
    return(journeysList)


def checkJourney(afcPos,afcDate,avlTDate):
    journey = afcDate.iloc[afcPos]
    inSt = np.array([journey['inStIndex'+str(k)] for k in range(journey['numTransfers']+1)],dtype=int)
    outSt = np.array([journey['outStIndex'+str(k)] for k in range(journey['numTransfers']+1)],dtype=int)
    lines = np.array([journey['lineIndex'+str(k)] for k in range(journey['numTransfers']+1)],dtype=int)
    tripIds = np.array([journey['tripId'+str(k)] for k in range(journey['numTransfers']+1)],dtype=int)
    
    separateLegs = []
    removeLegs = []
    for j in range(journey['numTransfers']+1):
#        date = str(inTime[j].date())
        
        # Tap-in/out at same station
        sameStInOut = inSt[j]==outSt[j]
        if sameStInOut: #same station tap in/out
            removeLegs += [j]
            continue
        
        # If this is last leg, no other check needed
        if j==journey['numTransfers']+1-1:
            continue
        
        # Should take first possible vehicle of next line
        (actualTT,plausibleTT) = getTransferTimes(avlTDate,tripIds,inSt,outSt,lines,j)
        if (actualTT==-99999): #check for when more than one plausible TT values passed (happens on line 3; ugghh)
            print(afcPos,journey['date'])
            continue
        
        if (len(actualTT)==0)|(len(plausibleTT)==0): #transfer doesn't exist because perhaps across days
            separateLegs +=[j]
            continue
        
        if actualTT>plausibleTT: #transfer time is more than that for a plausible first vehicle transfer
            separateLegs +=[j]
            continue
        
        # Transfer to same line but valid (possibly due to short-turning)
#        sameLineValid = lines[j]==lines[j+1] #same line; valid because first next vehicle taken
#        if sameLineValid: #valid same line transfer
#            removeLegs += [j]
#            continue
#    if (len(removeLegs)>0) & (len(separateLegs)==0):
#        print(afcPos)
    if (removeLegs==[]) & (separateLegs==[]):
        newJourneys = [journey.tolist()]
    else:
        newJourneys = createNewJourneys(separateLegs,removeLegs,journey,inSt,outSt,lines,tripIds)
    
    return(newJourneys)

def checkJourneysOnDate(date):
    afcName = './vars/afcDate/afc'+date[-2:]+'.h5'
    avlName = './vars/avlDate/avl'+date[-2:]+'.h5'
    afcDate = pd.read_hdf(afcName)#afc.loc[afc['date']==date]
    avlTDate = pd.read_hdf(avlName)#avlTransfers.loc[avlTransfers['date']==date]
#    avlTDate['dt'] = pd.to_datetime(avlTDate['dt'])
    allJourneys = []
    for afcPos in range(len(afcDate)):
#        print(str(afcPos)+'/'+str(len(afcDate)))
        allJourneys += checkJourney(afcPos,afcDate,avlTDate)
    afterInference = pd.DataFrame(allJourneys)
    afterInference.columns = ['date','numTransfers','orStIndex','destStIndex',
           'tripId0','tripId1','tripId2','tripId3',
           'inStIndex0','inStIndex1','inStIndex2','inStIndex3',
           'outStIndex0','outStIndex1','outStIndex2','outStIndex3',
           'lineIndex0','lineIndex1','lineIndex2','lineIndex3',
           'inTime0','inTime1','inTime2','inTime3',
           'outTime0','outTime1','outTime2','outTime3']
    afterName = './vars/afterInferenceDate/afterInference'+date[-2:]+'.h5' #(previously afterDate/after+...)
    afterInference.to_hdf(afterName,key='df',mode='w')
    return(allJourneys)


#%% Main
if __name__ == '__main__':
    print('Pool begins')
    start_time = timefxn.time()
    
    with Pool(40) as pool:
        allJourneys = zip(*pool.map(checkJourneysOnDate,
                                    pd.date_range(start='2015-03-01', end='2015-03-31').astype(str).to_list()))
        pool.close()
        pool.join()
    print("--- %s seconds ---" % (timefxn.time() - start_time))

allJourneys = list(chain(*allJourneys))
afterInference = pd.DataFrame(allJourneys)
afterInference.columns = ['date','numTransfers','orStIndex','destStIndex',
       'tripId0','tripId1','tripId2','tripId3',
       'inStIndex0','inStIndex1','inStIndex2','inStIndex3',
       'outStIndex0','outStIndex1','outStIndex2','outStIndex3',
       'lineIndex0','lineIndex1','lineIndex2','lineIndex3',
       'inTime0','inTime1','inTime2','inTime3',
       'outTime0','outTime1','outTime2','outTime3']
afterInference.to_hdf('./vars/afterInference.h5',key='df',mode='w')


#%% 
date = '2015-03-01'
afterName = './vars/afterInferenceDate/afterInference'+date[-2:]+'.h5' #(previously afterDate/after+...)
finalAfter = pd.read_hdf(afterName)
for date in pd.date_range(start='2015-03-02', end='2015-03-31').astype(str).to_list():
    afterName = './vars/afterInferenceDate/afterInference'+date[-2:]+'.h5' #(previously afterDate/after+...)
    print(afterName)
    finalAfter = finalAfter.append(pd.read_hdf(afterName))
finalAfter.to_hdf('./vars/afcFinalInference.h5',key='df',mode='w') #(previously finalInference.h5)



#%% Non-pool
#    for date in afc['date'].unique():
##        date = afc['date'].unique()[0]
#        afcDate = afc.loc[afc['date']==date]
#        avlTDate = avlTransfers.loc[avlTransfers['date']==date]
#        start_time = timefxn.time()
#        allJourneys = []
#        for afcPos in range(500):#range(len(afcDate)):
#            print(str(afcPos)+'/'+str(len(afcDate)))
#            allJourneys += checkJourney(afcPos,afcDate,avlTDate)
#    print("--- %s seconds ---" % (timefxn.time() - start_time))
