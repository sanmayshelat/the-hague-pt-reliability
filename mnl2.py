### Use PandasBiogeme for MNL

#%% Imports
import pandas as pd
import biogeme.database as bioDb
import biogeme.biogeme as bioBio
import biogeme.models as bioModels
from biogeme.expressions import Beta
# dssd
#%% Clean up: Deletes previous models!
#import re
#oldModelName = 'mnl'
modelName = 'op_s_1'
#import os
#try:
#    os.remove(oldModelName+'.html')
#    os.remove(oldModelName+'.pickle')
#    os.remove('headers.py')
#except:
#    print('Previous model doesn\'t exist')

#%% Choice data
# Get
results = pd.read_hdf('./vars/biogemeDf_op.h5')
#results = results.loc[results['hour']>6]
#results = results.sample(frac=0.7)
maxAlts = results['numAlts'].max()
# oTramCols = ['oTram_'+str(i) for i in range(maxAlts)]
# tTramCols = ['tTram_'+str(i) for i in range(maxAlts)]
# oBusCols = ['oBus_'+str(i) for i in range(maxAlts)]
# tBusCols = ['tBus_'+str(i) for i in range(maxAlts)]
#results = results.loc[(np.all((results[oTramCols]==1)|
#                              (results[oTramCols]==-1),axis=1)) & 
#                      (np.all((results[tTramCols]==1)|
#                              (results[tTramCols]==-1),axis=1))]
#results = results.loc[(np.all((results[oBusCols]==1)|
#                              (results[oBusCols]==-1),axis=1)) & 
#                      (np.all((results[tBusCols]==1)|
#                              (results[tBusCols]==-1),axis=1))]
#results = results.loc[(~np.any(results[oBusCols]==1,axis=1))&
#                      (~np.any(results[tBusCols]==1,axis=1))]
#results = results.loc[(~np.any(results[oTramCols]==1,axis=1))&
#                      (~np.any(results[tTramCols]==1,axis=1))]
#results = results.loc[(~np.any(results[tBusCols]==1,axis=1))]
#sdf
#topDispersionOds = results.loc[(results['owtVar_1']-results['owtVar_0']).sort_values(ascending=False).index,['orStIndex','destStIndex']]
#results = results.loc[topDispersionOds.iloc[:25812].index]

#cond1 = ((results['owtStd_0']>results['owtStd_1'])&results['routeId']==0)
#cond2 = ((results['owtStd_0']<results['owtStd_1'])&results['routeId']==1)
#cond3 = ((results['owtStd_0']==results['owtStd_1']))
#results = results.loc[~(cond1|cond2|cond3)]

#results = results.loc[~((results['orStIndex']==202)&(results['destStIndex']))]
#results = results.loc[~((results['orStIndex']==202))]

#%% Store in Biogeme DB
database = bioDb.Database('results',results) 
#!!! also creates headers.py which imports biogeme.expressions completely !!!

globals().update(database.variables)

#%% Define parameters
#!!! Note the fixed values
bNTransfers	= Beta('bNTransfers',0,-1000,1000,0)
bLnPsl = Beta('bLnPsl',0,-1000,1000,0)

## Origin WT
#O-Plan
bOWtPlan = Beta('bOWtPlan',0,-1000,1000,1)
bBusOWtPlan = Beta('bBusOWtPlan',0,-1000,1000,0)
bTramOWtPlan = Beta('bTramOWtPlan',0,-1000,1000,0)
#O-50
bOWt50 = Beta('bOWt50',0,-1000,1000,1)
bBusOWt50 = Beta('bBusOWt50',0,-1000,1000,1)
bTramOWt50 = Beta('bTramOWt50',0,-1000,1000,1)
#O-Diff
bOWtDiff = Beta('bOWtDiff',0,-1000,1000,1)
bBusOWtDiff = Beta('bBusOWtDiff',0,-1000,1000,0)
bTramOWtDiff = Beta('bTramOWtDiff',0,-1000,1000,0)
#O-PosDiff
bOWtPosDiff = Beta('bOWtPosDiff',0,-1000,1000,1)
bBusOWtPosDiff = Beta('bBusOWtPosDiff',0,-1000,1000,1)
bTramOWtPosDiff = Beta('bTramOWtPosDiff',0,-1000,1000,1)
#O-NegDiff
bOWtNegDiff = Beta('bOWtNegDiff',0,-1000,1000,1)
bBusOWtNegDiff = Beta('bBusOWtNegDiff',0,-1000,1000,1)
bTramOWtNegDiff = Beta('bTramOWtNegDiff',0,-1000,1000,1)
#O-RBT
bOWtRbt = Beta('bOWtRbt',0,-1000,1000,1)
bBusOWtRbt = Beta('bBusOWtRbt',0,-1000,1000,1)
bTramOWtRbt = Beta('bTramOWtRbt',0,-1000,1000,1)
#O-RBI
bOWtRbi = Beta('bOWtRbi',0,-1000,1000,1)
bBusOWtRbi = Beta('bBusOWtRbi',0,-1000,1000,1)
bTramOWtRbi = Beta('bTramOWtRbi',0,-1000,1000,1)
#O-STD
bOWtStd = Beta('bOWtStd',0,-1000,1000,1)
bBusOWtStd = Beta('bBusOWtStd',0,-1000,1000,0)
bTramOWtStd = Beta('bTramOWtStd',0,-1000,1000,0)
#O-SKEW
bOWtSkew = Beta('bOWtSkew',0,-1000,1000,1)
bBusOWtSkew = Beta('bBusOWtSkew',0,-1000,1000,1)
bTramOWtSkew = Beta('bTramOWtSkew',0,-1000,1000,1)
#O-VAR
bOWtVar = Beta('bOWtVar',0,-1000,1000,1)
bBusOWtVar = Beta('bBusOWtVar',0,-1000,1000,1)
bTramOWtVar = Beta('bTramOWtVar',0,-1000,1000,1)

## Transfer WT
#T-Plan
bTWtPlan = Beta('bTWtPlan',0,-1000,1000,1)
bBusTWtPlan = Beta('bBusTWtPlan',0,-1000,1000,1)
bTramTWtPlan = Beta('bTramTWtPlan',0,-1000,1000,0)
#T-50
bTWt50 = Beta('bTWt50',0,-1000,1000,1)
bBusTWt50 = Beta('bBusTWt50',0,-1000,1000,1)
bTramTWt50 = Beta('bTramTWt50',0,-1000,1000,1)
#T-Diff
bTWtDiff = Beta('bTWtDiff',0,-1000,1000,1)
bBusTWtDiff = Beta('bBusTWtDiff',0,-1000,1000,1)
bTramTWtDiff = Beta('bTramTWtDiff',0,-1000,1000,0)
#T-PosDiff
bTWtPosDiff = Beta('bTWtPosDiff',0,-1000,1000,1)
bBusTWtPosDiff = Beta('bBusTWtPosDiff',0,-1000,1000,1)
bTramTWtPosDiff = Beta('bTramTWtPosDiff',0,-1000,1000,1)
#T-NegDiff
bTWtNegDiff = Beta('bTWtNegDiff',0,-1000,1000,1)
bBusTWtNegDiff = Beta('bBusTWtNegDiff',0,-1000,1000,1)
bTramTWtNegDiff = Beta('bTramTWtNegDiff',0,-1000,1000,1)
# T-RBT
bTWtRbt = Beta('bTWtRbt',0,-1000,1000,1)
bBusTWtRbt = Beta('bBusTWtRbt',0,-1000,1000,1)
bTramTWtRbt = Beta('bTramTWtRbt',0,-1000,1000,1)
#T-RBI
bTWtRbi = Beta('bTWtRbi',0,-1000,1000,1)
bBusTWtRbi = Beta('bBusTWtRbi',0,-1000,1000,1)
bTramTWtRbi = Beta('bTramTWtRbi',0,-1000,1000,1)
#T-STD
bTWtStd = Beta('bTWtStd',0,-1000,1000,1)
bBusTWtStd = Beta('bBusTWtStd',0,-1000,1000,1)
bTramTWtStd = Beta('bTramTWtStd',0,-1000,1000,1)
#T-SKEW
bTWtSkew = Beta('bTWtSkew',0,-1000,1000,1)
bBusTWtSkew = Beta('bBusTWtSkew',0,-1000,1000,1)
bTramTWtSkew = Beta('bTramTWtSkew',0,-1000,1000,1)
#T-VAR
bTWtVar = Beta('bTWtVar',0,-1000,1000,1)
bBusTWtVar = Beta('bBusTWtVar',0,-1000,1000,1)
bTramTWtVar = Beta('bTramTWtVar',0,-1000,1000,1)

##IVT
bTotalIvt = Beta('bTotalIvt',0,-1000,1000,1)
bBusIvt = Beta('bBusIvt',0,-1000,1000,0)
bTramIvt = Beta('bTramIvt',0,-1000,1000,0)

## Total WT
bTotalWt = Beta('bTotalWt',0,-1000,1000,1)
bBusWt = Beta('bBusWt',0,-1000,1000,1)
bTramWt = Beta('bTramWt',0,-1000,1000,1)

#%% Utility function
# Definitions
v0 = (bNTransfers*numTransfers_0 + bLnPsl*lnPsl_0 + 
      bTotalIvt*(oIvt_0+tIvt_0) + bTotalWt*(owtPlan_0+twtPlan_0) +
      bTramWt*(owtPlan_0*oTram_0+twtPlan_0*tTram_0) + bBusWt*(owtPlan_0*oBus_0+twtPlan_0*tBus_0) +
      bOWtPlan*(owtPlan_0) + bOWtDiff*(owtDiff_0) + bOWtPosDiff*(owtPosDiff_0) + bOWtNegDiff*(owtNegDiff_0) + bOWtRbt*(owtRbt_0) + bOWtRbi*(owtRbi_0) + bOWtStd*(owtStd_0) + bOWtSkew*(owtSkew_0) + bOWtVar*(owtVar_0) +
      bTWtPlan*(twtPlan_0) + bTWtDiff*(twtDiff_0) + bTWtPosDiff*(twtPosDiff_0) + bTWtNegDiff*(twtNegDiff_0) + bTWtRbt*(twtRbt_0) + bTWtRbi*(twtRbi_0) + bTWtStd*(twtStd_0) + bTWtSkew*(twtSkew_0) + bTWtVar*(twtVar_0) +
      
      bTramIvt*(oIvt_0*oTram_0+tIvt_0*tTram_0) + bBusIvt*(oIvt_0*oBus_0+tIvt_0*tBus_0) +
      
      bTramOWtPlan*(owtPlan_0*oTram_0) + bBusOWtPlan*(owtPlan_0*oBus_0) +
      bTramOWt50*(owt50_0*oTram_0) + bBusOWt50*(owt50_0*oBus_0) +
      bTramOWtDiff*(owtDiff_0*oTram_0) + bBusOWtDiff*(owtDiff_0*oBus_0) +
      bTramOWtPosDiff*(owtPosDiff_0*oTram_0) + bBusOWtPosDiff*(owtPosDiff_0*oBus_0) +
      bTramOWtNegDiff*(owtNegDiff_0*oTram_0) + bBusOWtNegDiff*(owtNegDiff_0*oBus_0) +
      bTramOWtRbt*(owtRbt_0*oTram_0) + bBusOWtRbt*(owtRbt_0*oBus_0) +
      bTramOWtRbi*(owtRbi_0*oTram_0) + bBusOWtRbi*(owtRbi_0*oBus_0) +
      bTramOWtStd*(owtStd_0*oTram_0) + bBusOWtStd*(owtStd_0*oBus_0) +
      bTramOWtSkew*(owtSkew_0*oTram_0) + bBusOWtSkew*(owtSkew_0*oBus_0) +
      bTramOWtVar*(owtVar_0*oTram_0) + bBusOWtVar*(owtVar_0*oBus_0) +
      
      bTramTWtPlan*(twtPlan_0*tTram_0) + bBusTWtPlan*(twtPlan_0*tBus_0) +
      bTramTWt50*(twt50_0*tTram_0) + bBusTWt50*(twt50_0*tBus_0) +
      bTramTWtDiff*(twtDiff_0*tTram_0) + bBusTWtDiff*(twtDiff_0*tBus_0) +
      bTramTWtPosDiff*(twtPosDiff_0*tTram_0) + bBusTWtPosDiff*(twtPosDiff_0*tBus_0) +
      bTramTWtNegDiff*(twtNegDiff_0*tTram_0) + bBusTWtNegDiff*(twtNegDiff_0*tBus_0) +
      bTramTWtRbt*(twtRbt_0*tTram_0) + bBusTWtRbt*(twtRbt_0*tBus_0) +
      bTramTWtRbi*(twtRbi_0*tTram_0) + bBusTWtRbi*(twtRbi_0*tBus_0) +
      bTramTWtStd*(twtStd_0*tTram_0) + bBusTWtStd*(twtStd_0*tBus_0) +
      bTramTWtSkew*(twtSkew_0*tTram_0) + bBusTWtSkew*(twtSkew_0*tBus_0) +
      bTramTWtVar*(twtVar_0*tTram_0) + bBusTWtVar*(twtVar_0*tBus_0))

v1 = (bNTransfers*numTransfers_1 + bLnPsl*lnPsl_1 + 
      bTotalIvt*(oIvt_1+tIvt_1) + bTotalWt*(owtPlan_1+twtPlan_1) +
      bTramWt*(owtPlan_1*oTram_1+twtPlan_1*tTram_1) + bBusWt*(owtPlan_1*oBus_1+twtPlan_1*tBus_1) +
      bOWtPlan*(owtPlan_1) + bOWtDiff*(owtDiff_1) + bOWtPosDiff*(owtPosDiff_1) + bOWtNegDiff*(owtNegDiff_1) + bOWtRbt*(owtRbt_1) + bOWtRbi*(owtRbi_1) + bOWtStd*(owtStd_1) + bOWtSkew*(owtSkew_1) + bOWtVar*(owtVar_1) +
      bTWtPlan*(twtPlan_1) + bTWtDiff*(twtDiff_1) + bTWtPosDiff*(twtPosDiff_1) + bTWtNegDiff*(twtNegDiff_1) + bTWtRbt*(twtRbt_1) + bTWtRbi*(twtRbi_1) + bTWtStd*(twtStd_1) + bTWtSkew*(twtSkew_1) + bTWtVar*(twtVar_1) +
      
      bTramIvt*(oIvt_1*oTram_1+tIvt_1*tTram_1) + bBusIvt*(oIvt_1*oBus_1+tIvt_1*tBus_1) +
      
      bTramOWtPlan*(owtPlan_1*oTram_1) + bBusOWtPlan*(owtPlan_1*oBus_1) +
      bTramOWt50*(owt50_1*oTram_1) + bBusOWt50*(owt50_1*oBus_1) +
      bTramOWtDiff*(owtDiff_1*oTram_1) + bBusOWtDiff*(owtDiff_1*oBus_1) +
      bTramOWtPosDiff*(owtPosDiff_1*oTram_1) + bBusOWtPosDiff*(owtPosDiff_1*oBus_1) +
      bTramOWtNegDiff*(owtNegDiff_1*oTram_1) + bBusOWtNegDiff*(owtNegDiff_1*oBus_1) +
      bTramOWtRbt*(owtRbt_1*oTram_1) + bBusOWtRbt*(owtRbt_1*oBus_1) +
      bTramOWtRbi*(owtRbi_1*oTram_1) + bBusOWtRbi*(owtRbi_1*oBus_1) +
      bTramOWtStd*(owtStd_1*oTram_1) + bBusOWtStd*(owtStd_1*oBus_1) +
      bTramOWtSkew*(owtSkew_1*oTram_1) + bBusOWtSkew*(owtSkew_1*oBus_1) +
      bTramOWtVar*(owtVar_1*oTram_1) + bBusOWtVar*(owtVar_1*oBus_1) +
      
      bTramTWtPlan*(twtPlan_1*tTram_1) + bBusTWtPlan*(twtPlan_1*tBus_1) +
      bTramTWt50*(twt50_1*tTram_1) + bBusTWt50*(twt50_1*tBus_1) +
      bTramTWtDiff*(twtDiff_1*tTram_1) + bBusTWtDiff*(twtDiff_1*tBus_1) +
      bTramTWtPosDiff*(twtPosDiff_1*tTram_1) + bBusTWtPosDiff*(twtPosDiff_1*tBus_1) +
      bTramTWtNegDiff*(twtNegDiff_1*tTram_1) + bBusTWtNegDiff*(twtNegDiff_1*tBus_1) +
      bTramTWtRbt*(twtRbt_1*tTram_1) + bBusTWtRbt*(twtRbt_1*tBus_1) +
      bTramTWtRbi*(twtRbi_1*tTram_1) + bBusTWtRbi*(twtRbi_1*tBus_1) +
      bTramTWtStd*(twtStd_1*tTram_1) + bBusTWtStd*(twtStd_1*tBus_1) +
      bTramTWtSkew*(twtSkew_1*tTram_1) + bBusTWtSkew*(twtSkew_1*tBus_1) +
      bTramTWtVar*(twtVar_1*tTram_1) + bBusTWtVar*(twtVar_1*tBus_1))

v2 = (bNTransfers*numTransfers_2 + bLnPsl*lnPsl_2 + 
      bTotalIvt*(oIvt_2+tIvt_2) + bTotalWt*(owtPlan_2+twtPlan_2) +
      bTramWt*(owtPlan_2*oTram_2+twtPlan_2*tTram_2) + bBusWt*(owtPlan_2*oBus_2+twtPlan_2*tBus_2) +
      bOWtPlan*(owtPlan_2) + bOWtDiff*(owtDiff_2) + bOWtPosDiff*(owtPosDiff_2) + bOWtNegDiff*(owtNegDiff_2) + bOWtRbt*(owtRbt_2) + bOWtRbi*(owtRbi_2) + bOWtStd*(owtStd_2) + bOWtSkew*(owtSkew_2) + bOWtVar*(owtVar_2) +
      bTWtPlan*(twtPlan_2) + bTWtDiff*(twtDiff_2) + bTWtPosDiff*(twtPosDiff_2) + bTWtNegDiff*(twtNegDiff_2) + bTWtRbt*(twtRbt_2) + bTWtRbi*(twtRbi_2) + bTWtStd*(twtStd_2) + bTWtSkew*(twtSkew_2) + bTWtVar*(twtVar_2) +
      
      bTramIvt*(oIvt_2*oTram_2+tIvt_2*tTram_2) + bBusIvt*(oIvt_2*oBus_2+tIvt_2*tBus_2) +
      
      bTramOWtPlan*(owtPlan_2*oTram_2) + bBusOWtPlan*(owtPlan_2*oBus_2) +
      bTramOWt50*(owt50_2*oTram_2) + bBusOWt50*(owt50_2*oBus_2) +
      bTramOWtDiff*(owtDiff_2*oTram_2) + bBusOWtDiff*(owtDiff_2*oBus_2) +
      bTramOWtPosDiff*(owtPosDiff_2*oTram_2) + bBusOWtPosDiff*(owtPosDiff_2*oBus_2) +
      bTramOWtNegDiff*(owtNegDiff_2*oTram_2) + bBusOWtNegDiff*(owtNegDiff_2*oBus_2) +
      bTramOWtRbt*(owtRbt_2*oTram_2) + bBusOWtRbt*(owtRbt_2*oBus_2) +
      bTramOWtRbi*(owtRbi_2*oTram_2) + bBusOWtRbi*(owtRbi_2*oBus_2) +
      bTramOWtStd*(owtStd_2*oTram_2) + bBusOWtStd*(owtStd_2*oBus_2) +
      bTramOWtSkew*(owtSkew_2*oTram_2) + bBusOWtSkew*(owtSkew_2*oBus_2) +
      bTramOWtVar*(owtVar_2*oTram_2) + bBusOWtVar*(owtVar_2*oBus_2) +
      
      bTramTWtPlan*(twtPlan_2*tTram_2) + bBusTWtPlan*(twtPlan_2*tBus_2) +
      bTramTWt50*(twt50_2*tTram_2) + bBusTWt50*(twt50_2*tBus_2) +
      bTramTWtDiff*(twtDiff_2*tTram_2) + bBusTWtDiff*(twtDiff_2*tBus_2) +
      bTramTWtPosDiff*(twtPosDiff_2*tTram_2) + bBusTWtPosDiff*(twtPosDiff_2*tBus_2) +
      bTramTWtNegDiff*(twtNegDiff_2*tTram_2) + bBusTWtNegDiff*(twtNegDiff_2*tBus_2) +
      bTramTWtRbt*(twtRbt_2*tTram_2) + bBusTWtRbt*(twtRbt_2*tBus_2) +
      bTramTWtRbi*(twtRbi_2*tTram_2) + bBusTWtRbi*(twtRbi_2*tBus_2) +
      bTramTWtStd*(twtStd_2*tTram_2) + bBusTWtStd*(twtStd_2*tBus_2) +
      bTramTWtSkew*(twtSkew_2*tTram_2) + bBusTWtSkew*(twtSkew_2*tBus_2) +
      bTramTWtVar*(twtVar_2*tTram_2) + bBusTWtVar*(twtVar_2*tBus_2))

v3 = (bNTransfers*numTransfers_3 + bLnPsl*lnPsl_3 + 
      bTotalIvt*(oIvt_3+tIvt_3) + bTotalWt*(owtPlan_3+twtPlan_3) +
      bTramWt*(owtPlan_3*oTram_3+twtPlan_3*tTram_3) + bBusWt*(owtPlan_3*oBus_3+twtPlan_3*tBus_3) +
      bOWtPlan*(owtPlan_3) + bOWtDiff*(owtDiff_3) + bOWtPosDiff*(owtPosDiff_3) + bOWtNegDiff*(owtNegDiff_3) + bOWtRbt*(owtRbt_3) + bOWtRbi*(owtRbi_3) + bOWtStd*(owtStd_3) + bOWtSkew*(owtSkew_3) + bOWtVar*(owtVar_3) +
      bTWtPlan*(twtPlan_3) + bTWtDiff*(twtDiff_3) + bTWtPosDiff*(twtPosDiff_3) + bTWtNegDiff*(twtNegDiff_3) + bTWtRbt*(twtRbt_3) + bTWtRbi*(twtRbi_3) + bTWtStd*(twtStd_3) + bTWtSkew*(twtSkew_3) + bTWtVar*(twtVar_3) +
      
      bTramIvt*(oIvt_3*oTram_3+tIvt_3*tTram_3) + bBusIvt*(oIvt_3*oBus_3+tIvt_3*tBus_3) +
      
      bTramOWtPlan*(owtPlan_3*oTram_3) + bBusOWtPlan*(owtPlan_3*oBus_3) +
      bTramOWt50*(owt50_3*oTram_3) + bBusOWt50*(owt50_3*oBus_3) +
      bTramOWtDiff*(owtDiff_3*oTram_3) + bBusOWtDiff*(owtDiff_3*oBus_3) +
      bTramOWtPosDiff*(owtPosDiff_3*oTram_3) + bBusOWtPosDiff*(owtPosDiff_3*oBus_3) +
      bTramOWtNegDiff*(owtNegDiff_3*oTram_3) + bBusOWtNegDiff*(owtNegDiff_3*oBus_3) +
      bTramOWtRbt*(owtRbt_3*oTram_3) + bBusOWtRbt*(owtRbt_3*oBus_3) +
      bTramOWtRbi*(owtRbi_3*oTram_3) + bBusOWtRbi*(owtRbi_3*oBus_3) +
      bTramOWtStd*(owtStd_3*oTram_3) + bBusOWtStd*(owtStd_3*oBus_3) +
      bTramOWtSkew*(owtSkew_3*oTram_3) + bBusOWtSkew*(owtSkew_3*oBus_3) +
      bTramOWtVar*(owtVar_3*oTram_3) + bBusOWtVar*(owtVar_3*oBus_3) +
      
      bTramTWtPlan*(twtPlan_3*tTram_3) + bBusTWtPlan*(twtPlan_3*tBus_3) +
      bTramTWt50*(twt50_3*tTram_3) + bBusTWt50*(twt50_3*tBus_3) +
      bTramTWtDiff*(twtDiff_3*tTram_3) + bBusTWtDiff*(twtDiff_3*tBus_3) +
      bTramTWtPosDiff*(twtPosDiff_3*tTram_3) + bBusTWtPosDiff*(twtPosDiff_3*tBus_3) +
      bTramTWtNegDiff*(twtNegDiff_3*tTram_3) + bBusTWtNegDiff*(twtNegDiff_3*tBus_3) +
      bTramTWtRbt*(twtRbt_3*tTram_3) + bBusTWtRbt*(twtRbt_3*tBus_3) +
      bTramTWtRbi*(twtRbi_3*tTram_3) + bBusTWtRbi*(twtRbi_3*tBus_3) +
      bTramTWtStd*(twtStd_3*tTram_3) + bBusTWtStd*(twtStd_3*tBus_3) +
      bTramTWtSkew*(twtSkew_3*tTram_3) + bBusTWtSkew*(twtSkew_3*tBus_3) +
      bTramTWtVar*(twtVar_3*tTram_3) + bBusTWtVar*(twtVar_3*tBus_3))


# Associate function with number in choice column
utilityFxns = {0:v0,1:v1,2:v2,3:v3}

# Associate availability columns/if all available define as below
avail = {0:av_0,1:av_1,2:av_2,3:av_3}

#%% Define estimation
logprob = bioModels.loglogit(utilityFxns,avail,routeId) # last argument is choice column name in dataset
biogeme  = bioBio.BIOGEME(database,logprob)
biogeme.modelName = modelName
estimates = biogeme.estimate()

#%% Result presentation and storage
betas = estimates.getBetaValues()
for k,v in betas.items():
    print(f"{k}=\t{v:.3g}")

stats = estimates.getGeneralStatistics()
print(stats['Init log likelihood'],stats['Final log likelihood'])
#for k,v in stats.items():
#    print(f"{k}=\t{v}")

pandasResults = estimates.getEstimatedParameters()
print(pandasResults)











