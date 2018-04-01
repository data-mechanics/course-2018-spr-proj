# Library data clean up 

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas
import math
import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import xmltodict

class city_scores_library_cleanup(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = []
    writes = ["bemullen_crussack_dharmesh_vinwah.libraries"] # change this reference everywhere else too

# here we are reading the file

d = pandas.read_csv('rptcityscoresummary.csv') 

data_new = d[d['CTY_SCR_NAME'] == 'LIBRARY USERS']

data_new = data_new[['CTY_SCR_NBR_DY_01','CTY_SCR_NBR_WK_01','ETL_LOAD_DATE','CTY_SCR_WEEK','CTY_SCR_DAY','CTY_SCR_DAY_NAME',]]

def date_cleaner(row):
    date = str(row['ETL_LOAD_DATE'])
    return date[:10]

data_new['ETL_LOAD_DATE'] = data_new.apply(lambda row: date_cleaner(row),axis=1)

def date_examiner(row): # expand to bucketing if students are in session or not!
    date = row['ETL_LOAD_DATE']
    month = date[5:7]
    day = date[8:10]
    if month in ['09']:
        if day in ['30','01','02','03','04','05','06','07','08','09']:
            return 1
        else:
            return 0
    elif month in ['10']:
        if day in ['01','02','03','04','05','06','07','08','09','10']:
            return 2
        else:
            return 0  
    elif month in ['08']:
        if day in ['30']:
            return 1
        else: return 0
    elif month in ['12']:
        if day in ['22','23','24','25','26','27','28','29','30','31']:
            return 2
        else:
            return 0
    elif month in ['06','02']:
        if day in ['01','02','03','04','05','06','07','08','09','10']:
            return 1
        else:
            return 0
    
    else:
        return 0
    
data_new['week_flag'] = data_new.apply(lambda row: date_examiner(row),axis=1)

data_student = data_new[data_new['week_flag'] == 1]
data_none = data_new[data_new['week_flag'] == 2]

mean = data_student.apply(np.sum)

mean = (mean['CTY_SCR_DAY'] /54)

mean2 = data_none.apply(np.sum)

mean2 = (mean2['CTY_SCR_DAY'] / 26)

plt.scatter(x = data_student['CTY_SCR_DAY'], y = data_student['CTY_SCR_NBR_DY_01'],)
plt.scatter(x = data_none['CTY_SCR_DAY'], y = data_none['CTY_SCR_NBR_DY_01'],)
plt.title('Plot')
plt.ylabel('Visitors to Library')
plt.xlabel('City Score')
#plt.show()

def session(row): # expand to bucketing if students are in session or not!
    date = row['ETL_LOAD_DATE']
    month = date[5:7]
    day = date[8:10]
    
    if month in ['01']: #january
        if day in ['01','02','03','04','05','06','07','08','09','10','11']:
            return 0
        else:
            return 1
    elif month in ['12','05']: #december and may
        if day in ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18']:
            return 1
        else:
            return 0  
    elif month in ['06','07','08']:
        return 0
        
    elif month in ['09','10','11','06','02','03','04',]:
        return 1
    else:
        return 0
    
data_new['students'] = data_new.apply(lambda row: session(row),axis=1 )

student = data_new[data_new['students'] == 1]
none = data_new[data_new['students'] == 0]

red_patch = mpatches.Patch(color='red', label='University out of session')
green_patch = mpatches.Patch(color='green', label='University in session')
plt.legend(handles=[red_patch,green_patch])

plt.scatter(x = student['CTY_SCR_DAY'], y = student['CTY_SCR_NBR_DY_01'],)
plt.scatter(x = none['CTY_SCR_DAY'], y = none['CTY_SCR_NBR_DY_01'],)
plt.show()

student_ = student[['CTY_SCR_DAY','CTY_SCR_NBR_DY_01']]
none_ = none[['CTY_SCR_DAY','CTY_SCR_NBR_DY_01']]

# ----------
# uncomment these to save CSV:
# ----------

# What I need to do is go through and make sure that I have this in the standard format with DML 
# (look at RetrieveCityScores.py between key =... and ...repo.logout(). Also, upload the ds non and student to 
# cspeople/dharmesh/cs591 so that they are up ! )
