#!/usr/bin/env python
# coding: utf-8

# # Gears and Parts Creation for Importing Data

# In[1]:


import os
import datetime
import wget
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine


# In[2]:


# Generating URL

import datetime

def getBSEBhav(x):
    m = x.strftime("%m")
    y = x.strftime("%y")
    d = x.strftime("%d")
    
    l = "https://www.bseindia.com/download/Bhavcopy/Equity/EQ{}{}{}_CSV.ZIP".format(d,m,y)
    
    return l


# In[3]:


# importing URL data

import wget
try:
    filename = wget.download(url)
    print(filename)
except:
    pass


# In[4]:


# DDownloading zip file using BSE url

def BSEBhav(x):
    a =  getBSEBhav(x)
    filename = wget.download(a)


# In[5]:


#creating directories

def makemydir(whatever):
  try:
    os.makedirs(whatever)
  except OSError:
    pass


#set directory
d = 'C:\\Users\\ajayd'

def create_BSE_folder(d):
    #creating Equity folder
    name = "Equity Data 2"
    makemydir(name)
    base_folder_add = d+"\\"+name
    os.chdir(base_folder_add)
    
    name = "BSE"
    makemydir(name)
    BSE_folder_add = base_folder_add+"\\"+name
    os.chdir(BSE_folder_add)
    
    name = "BSE_Zipped"
    makemydir(name)
    BSE_zip = BSE_folder_add + "\\"+name
    
    name = "BSE_Unzipped"
    makemydir(name)
    BSE_unzip = BSE_folder_add + "\\"+name
    
    return base_folder_add,BSE_folder_add,BSE_zip, BSE_unzip


# In[6]:


# handling imported zip file 

# importing required modules 
from zipfile import ZipFile

file_add = "Address"
# specifying the zip file name 
file_name = file_add
  

# Quick reponse function  
    
def unzipping(Zip,Unzip):
    os.chdir(Zip)
    a = os.getcwd()
    l = list(os.listdir())
    l = [a+"\\"+s for s in l]

    for i in l:
        n = i
        with ZipFile(n, 'r') as zip: 
            zip.printdir() 
            print('Extracting all the files now...') 
            zip.extractall(Unzip)
            zip.close()
            print('Done!') 


# # Putting it together

# In[7]:


#set directory
d = 'C:\\Users\\ajayd'

from datetime import date, timedelta
edate = datetime.datetime(2020, 1, 19)   # end date
sdate = datetime.datetime(2019, 1, 19)   # start date

def get_BSEdata(d,sdate,edate):
    equity, BSE, Zip, Unzip = create_BSE_folder(d)
    
    # getting zip files
    os.chdir(Zip)
    delta = edate - sdate 
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        try:
            BSEBhav(day)
            print("{} Succefully imported.".format(day))
        except:
            print("data {} not available .".format(day))
            pass
    
    unzipping(Zip,Unzip)
    print("Done")
    return Zip, Unzip

Zip, Unzip = get_BSEdata(d,sdate,edate)


# # Creating DataBase

# In[8]:


# decoding file name to date
def filename2date(a):
    d = int(a[2:4])
    m = int(a[4:6])
    y = int("20"+ a[6:8])
    x = datetime.datetime(y, m,d)
    return x


# In[9]:


# Getting unzip file csvs to SQL
#change direc
os.chdir(Unzip)

XL_files = list(os.listdir())

#Creating engine
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)
#Database Name 
DB = "DB"

#looping over files
for i in range(len(XL_files)):
    xl = XL_files[i]
    d = Unzip
    xls = pd.read_csv(xl)
    date = filename2date(xl)
    xls["DATE"] = date.strftime('%b %d,%Y')
    xls.to_sql(name=DB,con=engine,if_exists='append',index=False)


# # SHOOTING QUERY

# In[21]:


query = '''
SELECT CLOSE,DATE,SC_NAME
FROM DB
WHERE DB.SC_CODE IN (500010, 532215,532174);
'''
df = pd.read_sql_query(query,engine)

df = df.set_index("DATE")
df = pd.pivot_table(df, index = "DATE",columns = "SC_NAME")
##QUICK DATA VIZ
from pylab import plt
plt.style.use('seaborn')
get_ipython().run_line_magic('matplotlib', 'inline')
title = "BANK STOCKS"
df.plot(figsize=(20, 10),title = title)

