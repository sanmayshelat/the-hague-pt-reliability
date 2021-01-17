### Separate data files by date
# Separate AFC, AVL datafiles by date for easier processing in transfer inference

#%% Imports
import pandas as pd

#%%
if __name__ == '__main__':
#%% Load data
    afc = pd.read_hdf('./vars/afcTripsWithTransfers.h5')
    afc['date'] = afc['inTime0'].astype(str).str[:10]
    
    cols = ['stop1','stop2','l1','l2','actualL1Id','actualL1Time','plausibleTT']
    avlTransfers = pd.read_csv('./vars/transferWtActual.csv.gz',usecols=cols)
    avlTransfers = avlTransfers.loc[~avlTransfers['actualL1Time'].isna()]
    
    
    avlTransfers['actualL1Time'] = pd.to_datetime(avlTransfers['actualL1Time'].str[:-6]).dt.tz_localize('CET')
    
    #avlTransfers['dt'] = pd.to_datetime(avlTransfers['actualL1Time'].str[:19])
    avlTransfers['day'] = avlTransfers['actualL1Time'].dt.dayofweek
    avlTransfers['hour'] = avlTransfers['actualL1Time'].dt.hour
    avlTransfers['date'] = avlTransfers['actualL1Time'].dt.date.astype(str)
    
    for date in afc['date'].unique():
        afcDate = afc.loc[afc['date']==date]
        avlTDate = avlTransfers.loc[avlTransfers['date']==date]
        afcName = './vars/afcDate/afc'+date[-2:]+'.h5'
        afcDate.to_hdf(afcName,key='df',mode='w')
        avlName = './vars/avlDate/avl'+date[-2:]+'.h5'
        avlTDate.to_hdf(avlName,key='df',mode='w')