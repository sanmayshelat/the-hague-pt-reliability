### Choice set formation
# Clustering of origin/destination stations defined here
# Route defined here
# Suitability of observation for analysis defined here

# Previously choiceSetSelect2.py

#%% Imports
import numpy as np
import pandas as pd
import pickle
with open('./vars/stopLines.pkl','rb') as f:# so that extract routes doesn't have to run every time
    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)
indexStation = uniqueStations[['stIndex','parent_station']].set_index('parent_station')

sdd
#%% Import final AFC set
afc = pd.read_hdf('./vars/afcFinalWSameLineTransfer.h5')
afc['day'] = afc['inTime0'].dt.dayofweek
afc['hour'] = afc['inTime0'].dt.hour
#afc1=afc.copy()
# Conditions
condTime = (afc['hour']>=9)&(afc['hour']<=15)
condDay = afc['day']<5
condNotDates = ~(afc['date']=='2015-03-06')
afc = afc.loc[condTime&condDay&condNotDates]
#afc = afc.loc[condNotDates]


afc = afc.fillna(-1) # because groupby ignores nans
afc1=afc.copy()
#afc=afc1.copy()
#cdfds
#%% Column grouping definitions
routeDefinition = ['numTransfers','orStIndex','destStIndex',
                   'inStIndex0','inStIndex1','inStIndex2','inStIndex3',
                   'outStIndex0','outStIndex1','outStIndex2','outStIndex3',
                   'lineIndex0','lineIndex1','lineIndex2','lineIndex3']
odCols = ['orStIndex','destStIndex']
pspaceInSeqCols = ['inStIndex0','inStIndex1','inStIndex2','inStIndex3']
pspaceOutSeqCols = ['outStIndex0','outStIndex1','outStIndex2','outStIndex3']
lineSeqCols = ['lineIndex0','lineIndex1','lineIndex2','lineIndex3']
#routeDefinition = routeDefinition + lineSeqCols
#%% Possible route choice set
minCss = 2
minOdObs = 200 #!!! This number doesn't make sense if we use all AFC data (no day/time filteration)
minChoicePerc = 0.1

afcRoutes = []
a = -1
while len(afcRoutes)!=a: # Keep filtering until number of routes is steady
    a = len(afcRoutes)
    
    # Minimum choice set size = 2
    afcRoutes = afc[routeDefinition].drop_duplicates()
    uniqueRoutes = afcRoutes.groupby(odCols).size()
    eOds = uniqueRoutes.loc[uniqueRoutes>=minCss].reset_index()[odCols]
    afc = pd.merge(afc,eOds,on=odCols)
    
    # Minimum observations for OD = 100
    afcOds = afc.groupby(odCols).size()
    eOds = afcOds.loc[afcOds>=minOdObs].reset_index()[odCols]
    afc = pd.merge(afc,eOds,on=odCols)
    
    # Minimum observations for each choice = 10% of OD observations
#    afc = afc.fillna(-1) # because groupby ignores nans
#    afcRoutes = afc.groupby(routeDefinition).size() # gets obs per choice
#    afcRoutes.name = 'obs'
#    afcRoutes = afcRoutes.reset_index()
#    
#    afcRoutes['obsPerc'] = afcRoutes.groupby(odCols)['obs'].transform(lambda x: x/x.sum()) # get obs percentage
#    afcRoutes = afcRoutes.loc[afcRoutes['obsPerc']>=minChoicePerc] #filter
#    uniqueRoutes = afcRoutes.groupby(odCols).size() # check number of routes per OD
#    eOds = uniqueRoutes.loc[uniqueRoutes>=minCss].reset_index()[odCols]
#    afcRoutes = pd.merge(afcRoutes,eOds,on=odCols) # eligible OD-routes
#    afc = pd.merge(afc,afcRoutes[routeDefinition],on=routeDefinition) 


#%% Get sequence of traversed stations for all routes
# Obviates 'getTraversedPaths.py'
# Get GTFS Route Topos
import gtfsRouteExtractFxn as gtfsRoute
date = '20150305'
routeTopos = gtfsRoute.extractRoutesFromGtfs(int(date))[2]
routeTopos = routeTopos.reset_index()
extraStops = [[3,0,13,8104,'Zoetermeer, Centrum-West',8103,52.0606,4.48308,-1],
              [3,1,41,8127,'Zoetermeer, Centrum-West',8103,52.0606,4.48308,-1]]
routeTopos = routeTopos.append(pd.DataFrame(extraStops,columns=routeTopos.columns))
routeTopos = routeTopos.sort_values(['route_id','direction_id','stop_sequence']).reset_index(drop=True)
routeTopos['stIndex'] = indexStation.loc[routeTopos['parent_station'].values].values

# Get traversed stations
afcRoutes = afc.drop_duplicates(routeDefinition).set_index(odCols).sort_index().reset_index()
allTravSt = []
weirdAfcRoutes = []
for i in range(len(afcRoutes)):
    print(i,len(afcRoutes))
    pspaceInSeq = afcRoutes.iloc[i][pspaceInSeqCols]
    pspaceOutSeq = afcRoutes.iloc[i][pspaceOutSeqCols]
    lineSeq = afcRoutes.iloc[i][lineSeqCols]
    travSt = []
    for j in range(afcRoutes.iloc[i]['numTransfers']+1):
        if i in weirdAfcRoutes:
            continue
        
        route = uniqueLines.loc[int(lineSeq[j])]['actRoute']
        direction = uniqueLines.loc[int(lineSeq[j])]['direction_id']
        origin = uniqueStations.loc[int(pspaceInSeq[j])]['stIndex']
        dest = uniqueStations.loc[int(pspaceOutSeq[j])]['stIndex']
        
        condLine = routeTopos['route_id']==route
        condDir = routeTopos['direction_id']==direction
        condDirOpp = routeTopos['direction_id']==np.abs(direction-1)
        
        route_rd = routeTopos.loc[condLine&condDir]
        route_rdopp = routeTopos.loc[condLine&condDirOpp]
        
        orSeqPos = route_rd.loc[route_rd['stIndex']==origin,'stop_sequence'].values
        if len(orSeqPos)==0:
            weirdAfcRoutes += [i]
            travSt += []
            continue
        
        destSeqPos = route_rd.loc[route_rd['stIndex']==dest,'stop_sequence'].values
        
        if (len(destSeqPos)==0) | (all(destSeqPos<=orSeqPos)):
            destSeqPos = route_rdopp.loc[route_rdopp['stIndex']==dest,'stop_sequence'].values
            if len(destSeqPos)==0:
                weirdAfcRoutes += [i]
                travSt += []
                continue
            
            if (len(orSeqPos)>1):
                orSeqPos = orSeqPos[-1]
            else:
                orSeqPos = orSeqPos[0]
            if (len(destSeqPos)>1):
                destSeqPos = destSeqPos[0]
            else:
                destSeqPos = destSeqPos[0]
            
            part1 = route_rd.loc[(route_rd['stop_sequence']>=orSeqPos),'stIndex'].to_list()
            part2 = route_rdopp.loc[(route_rdopp['stop_sequence']<=destSeqPos),'stIndex'].to_list()
            travSt += [pd.Series(part1+part2).drop_duplicates().tolist()]
            
        else:
            diffPos = destSeqPos - orSeqPos[:,np.newaxis]
            destSeqPos = destSeqPos[int(np.where(diffPos>0,diffPos,np.inf).argmin()/len(orSeqPos))] #!!! don't change orSeqPos first!
            orSeqPos = orSeqPos[np.where(diffPos>0,diffPos,np.inf).argmin()%len(orSeqPos)]
            
            travSt += [route_rd.loc[((route_rd['stop_sequence']>=orSeqPos)&
                                    (route_rd['stop_sequence']<=destSeqPos)),'stIndex'].to_list()]
    allTravSt += [travSt]

#allTravSt = pd.Series[allTravSt]
afcRoutes['travSt'] = allTravSt

#%% Filter out OD pairs that only have paths that are exactly the same
#allTravSt = allTravSt.loc[~allTravSt.astype(str).duplicated()]
afcRoutes['flatTravSt'] = [[j for j in i] for i in allTravSt]
#afcRoutes['travStStr'] = afcRoutes['flatTravSt'].astype(str)

afcRoutes['travStStr'] = afcRoutes['travSt'].astype(str)

eOds = (afcRoutes.groupby(odCols)['travStStr'].nunique()>1).reset_index()
eOds = eOds.loc[eOds['travStStr']].drop(columns='travStStr')
#afcRoutes = afcRoutes.loc[~afcRoutes['travSt'].astype(str).duplicated()]
afcRoutes = pd.merge(afcRoutes,eOds,on=odCols) # eligible OD-routes
afc = pd.merge(afc,afcRoutes[routeDefinition+['travSt','travStStr']],on=routeDefinition)

# Keep only those with overlapping paths
#eOds = (afcRoutes.groupby(odCols)['travStStr'].nunique()==1).reset_index()
#eOds = eOds.loc[eOds['travStStr']].drop(columns='travStStr')
#afcRoutes = pd.merge(afcRoutes,eOds,on=odCols) # eligible OD-routes
#afc = pd.merge(afc,afcRoutes[routeDefinition+['travSt','travStStr']],on=routeDefinition)

#%% New route definition
routeDefinition = ['numTransfers','orStIndex','destStIndex',
                   'inStIndex0','inStIndex1','inStIndex2','inStIndex3',
                   'outStIndex0','outStIndex1','outStIndex2','outStIndex3','travStStr']


#%% Filtering
afcRoutes = []
a = -1
while len(afcRoutes)!=a: # Keep filtering until number of routes is steady
    a = len(afcRoutes)
    
    # Minimum choice set size = 2
    afcRoutes = afc[routeDefinition].drop_duplicates()
    uniqueRoutes = afcRoutes.groupby(odCols).size()
    eOds = uniqueRoutes.loc[uniqueRoutes>=minCss].reset_index()[odCols]
    afc = pd.merge(afc,eOds,on=odCols)
    
    # Minimum observations for OD = 100
    afcOds = afc.groupby(odCols).size()
    eOds = afcOds.loc[afcOds>=minOdObs].reset_index()[odCols]
    afc = pd.merge(afc,eOds,on=odCols)
    
    # Minimum observations for each choice = 10% of OD observations
#    afc = afc.fillna(-1) # because groupby ignores nans
    afcRoutes = afc.groupby(routeDefinition).size() # gets obs per choice
    afcRoutes.name = 'obs'
    afcRoutes = afcRoutes.reset_index()
    
    afcRoutes['obsPerc'] = afcRoutes.groupby(odCols)['obs'].transform(lambda x: x/x.sum()) # get obs percentage
    afcRoutes = afcRoutes.loc[afcRoutes['obsPerc']>=minChoicePerc] #filter
    uniqueRoutes = afcRoutes.groupby(odCols).size() # check number of routes per OD
    eOds = uniqueRoutes.loc[uniqueRoutes>=minCss].reset_index()[odCols]
    afcRoutes = pd.merge(afcRoutes,eOds,on=odCols) # eligible OD-routes
    afc = pd.merge(afc,afcRoutes[routeDefinition],on=routeDefinition)


#%% Store
afc.to_hdf('./vars/afcSuitable_op.h5',key='df',mode='w') #(previously choiceSet.h5)
