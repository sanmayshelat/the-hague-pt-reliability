### Assign attributes to selected choice set
#!!! Check carefully
# Previously assignAttributesRel3.py

#%% Imports
import pandas as pd
import numpy as np
import pickle
with open('./vars/stopLines.pkl','rb') as f:# so that extract routes doesn't have to run every time
    (uniqueStations,numStations,uniqueLines,numLines,stationLines,
     lspaceWalk,stationDists,stationWalkingTimes) = pickle.load(f)
indexStation = uniqueStations[['stIndex','parent_station']].set_index('parent_station')
df
#%% Definitions
def toTravMat(lspaceSeq):
    mat = np.zeros([numStations,numStations])
    for i in range(len(lspaceSeq)-1):
        mat[lspaceSeq[i],lspaceSeq[i+1]] = 1
    return(mat)

#%% Import attributes
oAtts = pd.read_hdf('./vars/oAtts_p.h5')
tAtts = pd.read_hdf('./vars/tAtts_p.h5')

#%% Column groupings
routeDefinition = ['numTransfers','orStIndex','destStIndex',
                   'inStIndex0','inStIndex1','outStIndex0','outStIndex1',
                   'travStStr']
odCols = ['orStIndex','destStIndex']
pspaceInSeqCols = ['inStIndex0','inStIndex1']
pspaceOutSeqCols = ['outStIndex0','outStIndex1']
lineSeqCols = ['lineIndex0','lineIndex1']

#%% Import files
cs = pd.read_hdf('./vars/afcSuitable_p.h5')
cs = cs.loc[cs['numTransfers']<=1]

# Get AFC routes
afcRoutes = cs.groupby(routeDefinition)[lineSeqCols].agg(set).reset_index()
afcRoutes = pd.merge(afcRoutes,
                     cs[routeDefinition+['travSt']].drop_duplicates(routeDefinition),
                     on=routeDefinition,how='left')
afcRoutes = afcRoutes.sort_values(odCols)
maxTransfers = afcRoutes['numTransfers'].max()
if maxTransfers>1:
    print('ERROR')

#%% Assign route ids
t = [list(range(i)) for i in afcRoutes.groupby(odCols).size().values]
afcRoutes['routeId'] = [j for i in t for j in i]

#%% Get obs, obsPerc
#cs = cs.fillna(-1) # because groupby ignores nans
csRoutes = cs.groupby(routeDefinition).size() # gets obs per choice
csRoutes.name = 'obs'
csRoutes = csRoutes.reset_index()
csRoutes['obsPerc'] = csRoutes.groupby(odCols)['obs'].transform(lambda x: x/x.sum()) # get obs percentage

afcRoutes = pd.merge(afcRoutes,csRoutes[routeDefinition+['obs','obsPerc']],
                     on=routeDefinition,how='left')

#%% Final observations
mergeCols = routeDefinition+['routeId','obs','obsPerc']
cs = pd.merge(cs,afcRoutes[mergeCols],on=routeDefinition)

#%% Choice set table
#!!! Depends on routeId in afcRoutes maintaining order
keepCols = ['inStIndex0','inStIndex1','outStIndex0','outStIndex1', #route context/useful to assign other vars
            'travSt','travStStr', #route context/useful to assign other vars
            'numTransfers'] #time independent attributes
            
routeAlts = afcRoutes.loc[:,odCols+keepCols]

maxAlts = afcRoutes.groupby(odCols).size().max()
eOds = afcRoutes.groupby(odCols).size().reset_index()[odCols] # change to afcRoutes[odCols].drop_duplicates()
altsCols = [j+'_'+str(i) for i in range(maxAlts) for j in keepCols]
routesTable = pd.DataFrame(columns=altsCols,index=pd.MultiIndex.from_frame(eOds))
routesTable = routesTable.reset_index()

for i in range(len(routesTable)):
    origin = routesTable.iloc[i]['orStIndex']
    dest = routesTable.iloc[i]['destStIndex']
    routes = routeAlts.loc[((routeAlts['orStIndex']==origin)&
                            (routeAlts['destStIndex']==dest))]#tuple(eOds.iloc[i])]
    t = routes[keepCols].values.reshape((1,-1))[0]
    routesTable.iloc[i,2:2+len(t)] = t #2 because first 2 cols are origin and dest

alts = pd.merge(cs[odCols+['day','hour','routeId']],routesTable,on=odCols)
alts = alts.reset_index(drop=True)
#sadasd
#%% Assign time dependent trip-based attributes
# This also gives availability of alternatives
for i in range(maxAlts):
    suffix = '_'+str(i)
    attCols = ['wtPlan','wtDiff','wtPosDiff','wtNegDiff','wt50','wtStd','wtVar','wtRbt','wtRbi','wtSkew','Ivt','Tram','Bus']
    
    # Origin
    oColsRename = dict(zip(['o'+j for j in attCols],['o'+j+suffix for j in attCols]))
    alts = pd.merge(alts,oAtts,
                    left_on=['travStStr'+suffix,'day','hour'],
                    right_on = ['travStStr','day','hour'],
                    right_index=True,how='left').rename(oColsRename,axis=1)
    
    # Transfer
    tColsRename = dict(zip(['t'+j for j in attCols],['t'+j+suffix for j in attCols]))
    alts = pd.merge(alts,tAtts,
                    left_on=['travStStr'+suffix,'day','hour'],
                    right_on = ['travStStr','day','hour'],
                    right_index=True,how='left').rename(tColsRename,axis=1)
    
    ## Alt availability
    altDoesntExist = alts['numTransfers'+suffix].isna()
    condT = (((alts['numTransfers'+suffix]+1)>1) & 
             ((alts['twtPlan'+suffix].isna()) | 
              (alts['twt50'+suffix].isna()) |
              (alts['tIvt'+suffix].isna()))) # if there is a transfer (also implies alternative exists) and the planned or actual transfer time or tIvt is unavailable
    
    condO = ((~alts['numTransfers'+suffix].isna()) & 
             ((alts['owtPlan'+suffix].isna()) | 
              (alts['owt50'+suffix].isna()) | 
              (alts['oIvt'+suffix].isna()))) # if the alternative exists and the planned or actual original wt or oIvt is unavailable
    
    cond = altDoesntExist | condT | condO # something is wrong in first leg or in second leg
    alts['av'+suffix] = (~cond).astype(int)
    
    
    ## Set 0 to values that don't exist because no transfer
    alts.loc[(alts['numTransfers'+suffix]+1)==1,
             ['twtPlan'+suffix,'twt50'+suffix,'twtRbt'+suffix,'tIvt'+suffix,
              'twtRbi'+suffix,'twtStd'+suffix,'twtSkew'+suffix,'twtVar'+suffix,
              'twtDiff'+suffix]] = 0
    


# Assign number of alts available
avCols = ['av_'+str(i) for i in range(maxAlts)]
alts['numAlts'] = alts[avCols].sum(axis=1)
alts = alts.loc[alts['numAlts']>=2]

#%% Add time dependent route-based attributes
travStCols = ['travSt_'+str(_) for _ in range(maxAlts)]
selectionCols = altsCols+avCols
comparableCols = list(set(selectionCols)-set(travStCols))
sameAlts = alts[selectionCols].drop_duplicates(comparableCols)
# Path size factor
for i in range(len(sameAlts)):
    print(i,len(sameAlts))
    travMat = np.zeros([numStations,numStations,maxAlts])
    psl = []
    for j in range(maxAlts):
        suffix ='_'+str(j)
        if sameAlts.iloc[i]['av'+suffix]!=0:
            travSt = sameAlts.iloc[i]['travSt'+suffix]
            for k in range(sameAlts.iloc[i]['numTransfers'+suffix]+1):
                lspaceSeq = travSt[k]
                travMat[:,:,j] += toTravMat(lspaceSeq)
    
    travMat[travMat==0]=np.nan
    for j in range(maxAlts):
        suffix ='_'+str(j)
        if sameAlts.iloc[i]['av'+suffix]!=0:
            sameAlts.loc[sameAlts.index[i],'lnPsl'+suffix] = np.log(np.nansum(travMat[:,:,j]/np.nansum(travMat,2))/np.nansum(travMat[:,:,j]))

pslCols = ['lnPsl_'+str(_) for _ in range(maxAlts)]
alts = pd.merge(alts,sameAlts[comparableCols+pslCols],on=comparableCols,how='left')

#%% Add choice alternatives
attCols = ['numTransfers','oTram','tTram','oBus','tBus', # time independent
           'oIvt','tIvt', # time dependent leg-based
           'twtPlan','twt50','twtDiff','twtPosDiff','twtNegDiff','twtRbt','twtRbi','twtStd','twtSkew','twtVar',
           'owtPlan','owt50','owtDiff','owtPosDiff','owtNegDiff','owtRbt','owtRbi','owtStd','owtSkew','owtVar',
           'lnPsl', # time dependent route based
           'av']
biogemeCols = (['orStIndex', 'destStIndex','day','hour','routeId','numAlts']+
               [j+'_'+str(i) for i in range(maxAlts) for j in attCols])
biogemeDf = alts.loc[:,biogemeCols]
biogemeDf = biogemeDf.fillna(-1).reset_index(drop=True)
biogemeDf.to_hdf('./vars/biogemeDf_p.h5',key='df',mode='w')

