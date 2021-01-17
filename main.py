### The main file for Den Haag Tram waiting time reliability comparison

#%% Imports
import avlActual
import storeStopLines
import avlPlanned
#avlActualTransferWt_pool
#avlPlannedTransferWt_pool
import afcRides
import afcAvlLink
import afcRestructuring
import separateDatafiles
#import afcTransferInference_pool
import afcCombineAfterInference
import afcSuitableForAnalysis
import getAttributeValues
import assignAttributes
hj
#%% Run sequence

if __name__ == '__main__':
    
    #%% Data prep - AVL
    # Merge AVL files for standardized indices, get transfer waiting times
    avlActual.main_avlActual()
    storeStopLines.main_storeStopLines()
    avlPlanned.main_avlPlanned()
    #avlActualTransferWt_pool # pooled implementation needs its own main/cannot be called from another file?
    #avlPlannedTransferWt_pool # pooled implementation needs its own main/cannot be called from another file?
    
    #%% Data prep - AFC
    # Prepare AFC data, combine with AVL, apply transfer inference
    afcRides.main_afcRides()
    afcAvlLink.main_afcAvlLink()
    afcRestructuring.main_afcRestructuring()
    separateDatafiles.main_separateDatafiles()
    #afcTransferInference_pool # pooled implementation needs its own main/cannot be called from another file?
    afcCombineAfterInference.main_afcCombineAfterInference()
    
    #%% Choice analysis prep
    # Get observations suitable for analysis, get attributes, assign attributes
    afcSuitableForAnalysis.main_afcSuitableForAnalysis()
    # getAttributeValues.main_getAttributeValues() # replaced by corridorAttributes
    getRouteIvt.main_getRouteIvt()
    corridorAttributes.main_corridorAttributes()
    assignAttributes.main_assignAttributes()
    mnl2
    figures
    
    #%% Choice analysis
    