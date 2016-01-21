
##############################################################

# readData.py - Jesse Hahm

# Date: JANUARY 2015

#

# Purpose:  

# code to read in PISTON DENDROMETER DATA
# Create two sets datastreams / dataframes: one with raw data 
#(voltage readings from each sensor and datalogger voltage)
# called 'voltage'
# the other with derived micrometer displacement for good sensors
# called 'radius'
##############################################################
## Import required libraries
import functools
import pandas as pd
import itertools
import csv

def readCSV(allFiles, saveCSV):
    """ Read CSV files that record piston dendrometer displacement as voltage """ 
    dfs = dict() # create a blank dictionary

    for file_ in allFiles:
        # Key: filename; Value: pandas data frame
        dfs[file_] = pd.read_csv(file_, header=None, parse_dates={"Year" : [1]})
        # Parse the year / day of year / time column to a single datetime64 index
        dfs[file_].index =(dfs[file_].Year +
                           pd.to_timedelta(dfs[file_][2],unit='D') +  
                           pd.to_timedelta(dfs[file_][3]//100-1,unit='H') + 
                           #loggers on DLS; minus one hour to set to UTC-8
                           pd.to_timedelta(dfs[file_][3]%100,unit='m'))
        del dfs[file_]['Year'], dfs[file_][0], dfs[file_][2], dfs[file_][3]
        
        # Relabel columns with file name + column number + 1
        # to match the campbell SE channel in the data logger
        for column in dfs[file_]:
            dfs[file_].rename(columns = {column : file_ + 
                                         str(dfs[file_].columns.get_loc(column) + 1)}, 
                              inplace=True)
        
    # Merge each dataframe in the dictionary, by datetim stamp
    merge = functools.partial(pd.merge, left_index=True, right_index=True, how='outer')
    voltage = functools.reduce(merge, dfs.values())


	
	
    #Here we assign each column name as a DSID (data stream ID)
    #To do so, we need to do the following:
    # 1: Figure out which Site the file is from (based on filename)
    # 2: Figure out which slope the file is from (based on filename)
    # 3: We now know which datalogger we have, and a subset of those columns
    # 4: Finally, pair the SE Channel Data Logger (from column name) with DSID
    import re
    metadata = pd.read_csv('PISTON_DENDROMETER_METADATA.csv',sep=',', dtype=str)

    for column in voltage:
        SEC = re.findall(r'\d+', column) #grab the number from the column label
        SEC = int(SEC[0])
        if 'SAGE' in column:
            if 'NORTH' in column:
                metadataSubset = metadata[(metadata.Site == 'Sagehorn') &
                                          (metadata.Slope == 'N')]
                SEChannel = dict(zip(metadataSubset['SE Channel Data Logger'], 
                                     metadataSubset['Raw Voltage DSID'] ))  
                if str(SEC) in metadataSubset['SE Channel Data Logger'].values:
                    voltage.rename(columns = {column : SEChannel[str(SEC)]}, 
                                  inplace=True)
                else:
                    voltage.rename(columns = {column : 'SAGE_NORTH_VOLTAGE'}, 
                                  inplace=True)    
            else:
                metadataSubset = metadata[(metadata.Site == 'Sagehorn') &
                                          (metadata.Slope == 'S')]
                SEChannel = dict(zip(metadataSubset['SE Channel Data Logger'], 
                                     metadataSubset['Raw Voltage DSID'] ))  
                if str(SEC) in metadataSubset['SE Channel Data Logger'].values:
                    voltage.rename(columns = {column : SEChannel[str(SEC)]}, 
                                  inplace=True)
                else:
                    voltage.rename(columns = {column : 'SAGE_SOUTH_VOLTAGE'}, 
                                  inplace=True)     
        else:
            if 'NORTH' in column:
                metadataSubset = metadata[(metadata.Site == 'Rivendell') &
                                          (metadata.Slope == 'N')]
                SEChannel = dict(zip(metadataSubset['SE Channel Data Logger'], 
                                     metadataSubset['Raw Voltage DSID'] ))  
                if str(SEC) in metadataSubset['SE Channel Data Logger'].values:
                    voltage.rename(columns = {column : SEChannel[str(SEC)]}, 
                                  inplace=True)
                else:
                    voltage.rename(columns = {column : 'RIV_NORTH_VOLTAGE'}, 
                                  inplace=True)    
            else:
                metadataSubset = metadata[(metadata.Site == 'Rivendell') &
                                          (metadata.Slope == 'S')]
                SEChannel = dict(zip(metadataSubset['SE Channel Data Logger'], 
                                     metadataSubset['Raw Voltage DSID'] ))  
                if str(SEC) in metadataSubset['SE Channel Data Logger'].values:
                    voltage.rename(columns = {column : SEChannel[str(SEC)]}, 
                                  inplace=True)
                else:
                    voltage.rename(columns = {column : 'RIV_SOUTH_VOLTAGE'}, 
                                  inplace=True)     
            

    #voltage to radius in microns; start with blank dataframe
	radius = pd.DataFrame()
	
	# Dictionary to map raw voltage DSID to micron DSID
	dsid_dict = dict(zip(metadata['Raw Voltage DSID'], 
                         metadata['Micron Displacement DSID']))  
	# Dictionary to map raw voltage DSID to good or bad sensors
	include_dict = dict(zip(metadata['Raw Voltage DSID'], 
                            metadata['Include'])) 

    for column in voltage:
	    #only include good sensor voltages, not datalogger
        if (column in metadata['Raw Voltage DSID'].values):
            if (include_dict[column] == 'yes'):
               radius[dsid_dict[column]] = voltage[column]*4

    if saveCSV:
        voltage.to_csv('Data\merged_voltage.csv')
        radius.to_csv('Data\merged_radius.csv')
		
    return voltage, radius