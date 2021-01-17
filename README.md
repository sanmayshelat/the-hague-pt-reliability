# README #

Analysing the effects of waiting time reliability using smart card (containing both tap-in and tap-out information), automatic vehicle location, and GTFS data. 

The majority of the files are for combining these datasets (in the absence of common identifiers). 

A multinomial logit model is applied to test various reliability indicators and evaluate the impact of reliability at different time periods. 

See abstract below for more details about the study. Please contact s.shelat@tudelft.nl for the full (accepted) paper. 

If you use this, please cite: S Shelat, O Cats, N van Oort, JWC van Lint, 2021. Analysing the Role of Waiting Time Reliability in Public Transport Route Choice Using Smart Card Data, 100th Transportation Research Board Annual Meeting, Washington DC..

### Data Required ###

Uses smart card (AFC) and vehicle location (AVL) data. Note that format of these datasets often vary by region. Also uses GTFS files.


### How to: ###

* Run scripts in order given in the main file.
* Some scripts have a parallelised implementations because they take too long on their own. Apparently, these implementations cannot be called from a main file so they have to be run separately.
* Finally run mnl2 and prediction for choice model estimation and validation respectively. Change values in prediction as required or read in the biogeme output directly.


### Libraries ###

* See imports for common libraries (Pandas, NumPy, etc.)
* mnl2 is a multinomial logit model that uses PandasBiogeme libraries (https://biogeme.epfl.ch/). Please do not forget to cite them as well if you use this script.

### Abstract ###

Waiting time has been consistently shown to have significant impact on route choice behaviour in public transport networks. Furthermore, unreliability in this component may cause frustration and anxiety in travellers. Although the effect of travel time reliability has been studied extensively, most studies have used stated preferences which have disadvantages, such as inherent hypothetical bias. In this study, we use revealed preferences derived from passively collected smart card data to analyse the role of waiting time reliability in public transport route choice in The Hague. Waiting time reliability is studied as regular and irregular deviations from scheduled values, and a number of indicators for the latter are examined. Results indicate reliability ratios of 0.20–1.12; relatively low compared to literature, potentially adding to growing consensus that stated preferences tend to overestimate values. Small coefficients in combination with the overall reliable service means that unreliability plays a small role in travellers’ route choices in The Hague. Additionally, behaviour in morning peak and off-peak hours is contrasted and difference in reliability coefficients for different modes in the network, and for origin and transfer stops are reported.

