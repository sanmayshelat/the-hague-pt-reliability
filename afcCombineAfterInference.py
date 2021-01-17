### Combine afc final inference with no transfers
# Previously getCombinedAfc.py

#%% Imports
import pandas as pd
import numpy as np
aa
#%%
if __name__ == '__main__':
    #%% Import and connect inferred AFC with no-transfer AFC
    afc1 = pd.read_hdf('./vars/afcRestructured.h5')
    afc1 = afc1.loc[afc1['numTransfers']==0]
    afc1['date'] = afc1['inTime0'].astype(str).str[:10]
    
    afc2 = pd.read_hdf('./vars/afcFinalInference.h5')
    #z = afc2.copy()
    #afc2=z.copy()
    afc2 = afc2.reset_index(drop=True)
    # Combine transfers to same line as one trip
    cond = (afc2['lineIndex0']==afc2['lineIndex1'])&(afc2['inStIndex1']==afc2['outStIndex0'])
    a = afc2.loc[cond].copy()
    while any(cond):
        afc2.loc[cond,['lineIndex0','lineIndex1','lineIndex2']] = afc2.loc[cond,['lineIndex1','lineIndex2','lineIndex3']].values
        afc2.loc[cond,'lineIndex3'] = np.nan
        
        afc2.loc[cond,['inStIndex1','inStIndex2']] = afc2.loc[cond,['inStIndex2','inStIndex3']].values
        afc2.loc[cond,'inStIndex3'] = np.nan
        afc2.loc[cond,['outStIndex0','outStIndex1','outStIndex2']] = afc2.loc[cond,['outStIndex1','outStIndex2','outStIndex3']].values
        afc2.loc[cond,'outStIndex3'] = np.nan
        
        afc2.loc[cond,['inTime1','inTime2']] = np.array(afc2.loc[cond,['inTime2','inTime3']])
        afc2.loc[cond,'inTime3'] = np.nan
        afc2.loc[cond,['outTime0','outTime1','outTime2']] = np.array(afc2.loc[cond,['outTime1','outTime2','outTime3']])
        afc2.loc[cond,'outTime3'] = np.nan
        
        afc2.loc[cond,'numTransfers'] = afc2.loc[cond,'numTransfers'] - 1
        cond = (afc2['lineIndex0']==afc2['lineIndex1'])&(afc2['inStIndex1']==afc2['outStIndex0'])
    
    cond = (afc2['lineIndex1']==afc2['lineIndex2'])&(afc2['inStIndex2']==afc2['outStIndex1'])
    a = afc2.loc[cond].copy()
    while any(cond):
        afc2.loc[cond,['lineIndex1','lineIndex2']] = afc2.loc[cond,['lineIndex2','lineIndex3']].values
        afc2.loc[cond,'lineIndex3'] = np.nan
        
        afc2.loc[cond,['inStIndex2']] = afc2.loc[cond,['inStIndex3']].values
        afc2.loc[cond,'inStIndex3'] = np.nan
        afc2.loc[cond,['outStIndex1','outStIndex2']] = afc2.loc[cond,['outStIndex2','outStIndex3']].values
        afc2.loc[cond,'outStIndex3'] = np.nan
        
        afc2.loc[cond,['inTime2']] = np.array(afc2.loc[cond,['inTime3']])
        afc2.loc[cond,'inTime3'] = np.nan
        afc2.loc[cond,['outTime1','outTime2']] = np.array(afc2.loc[cond,['outTime2','outTime3']])
        afc2.loc[cond,'outTime3'] = np.nan
        
        afc2.loc[cond,'numTransfers'] = afc2.loc[cond,'numTransfers'] - 1
        cond = (afc2['lineIndex1']==afc2['lineIndex2'])&(afc2['inStIndex2']==afc2['outStIndex1'])
    
    cond = (afc2['lineIndex2']==afc2['lineIndex3'])&(afc2['inStIndex3']==afc2['outStIndex2'])
    a = afc2.loc[cond].copy()
    while any(cond):
        afc2.loc[cond,['lineIndex2']] = afc2.loc[cond,['lineIndex3']].values
        afc2.loc[cond,'lineIndex3'] = np.nan
        
        afc2.loc[cond,'inStIndex3'] = np.nan
        afc2.loc[cond,['outStIndex2']] = afc2.loc[cond,['outStIndex3']].values
        afc2.loc[cond,'outStIndex3'] = np.nan
        
        afc2.loc[cond,'inTime3'] = np.nan
        afc2.loc[cond,['outTime2']] = np.array(afc2.loc[cond,['outTime3']])
        afc2.loc[cond,'outTime3'] = np.nan
        
        afc2.loc[cond,'numTransfers'] = afc2.loc[cond,'numTransfers'] - 1
        cond = (afc2['lineIndex1']==afc2['lineIndex2'])&(afc2['inStIndex2']==afc2['outStIndex1'])
    
    
    
    afc = pd.concat([afc1,afc2],ignore_index=True)
    afc.to_hdf('./vars/afcFinal.h5',key='df',mode='w')
