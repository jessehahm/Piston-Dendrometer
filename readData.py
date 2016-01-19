
##############################################################

# readData.py - Jesse Hahm

# Date: JANUARY 2015

#

# Purpose:  

# code to read in PISTON DENDROMETER DATA

##############################################################
## Import required libraries
import functools
import pandas as pd
import itertools
import csv

def readCSV(allFiles, saveCSV):
    """ Read CSV files that record piston dendrometer displacement """ 
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
    radius = functools.reduce(merge, dfs.values())


    if saveCSV:
        radius.to_csv('Data\merged_radius.csv')

    return radius