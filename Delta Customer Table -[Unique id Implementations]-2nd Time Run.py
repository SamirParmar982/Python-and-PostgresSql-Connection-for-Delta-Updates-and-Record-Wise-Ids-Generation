#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#pip install pandas psycopg2


# In[ ]:


import pandas as pd
import psycopg2
import os
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from datetime import datetime
pd.options.display.max_columns = None
pd.options.display.max_rows = None


# In[ ]:


Connection_info = {
    "host": "localhost",
    "database": "operations_db",
    "user": "postgres",
    "password": "nj@12345"
}


# In[ ]:


connection = psycopg2.connect(**Connection_info)


# In[ ]:


cursor = connection.cursor()


# In[ ]:


df1 = pd.read_csv('company_cost_202310181702.csv')


# In[ ]:


df1.head()


# In[ ]:


df1['employeeid']= df1['employeeid'].astype('int64')
df1['cost']= df1['cost'].astype('int64')
df1['totalsales']= df1['totalsales'].astype('int64')
df1['totalamount']= df1['totalamount'].astype('int64')
df1['productname']= df1['productname'].astype('string')
df1['locations']= df1['locations'].astype('string')


# In[ ]:


df1.head()


# In[ ]:


#Checking the insert


# In[ ]:


connection = psycopg2.connect(**Connection_info)


# In[ ]:


cursor = connection.cursor()


# In[ ]:


query = "select * from company_cost"


# In[ ]:


df = pd.read_sql(query, connection)


# In[ ]:


df.head()


# In[ ]:


df.info()


# In[ ]:


df['productname'] = df['productname'].astype('string')
df['locations'] = df['locations'].astype('string')


# In[ ]:


df1.info()


# In[ ]:


# 1. detect changes. Get rows that are not present in the target.
changes = df[~df.apply(tuple,1).isin(df1.apply(tuple,1))]


# In[ ]:


changes.info()


# In[ ]:


# 2. Get new records
inserts = changes[~changes.employeeid.isin(df1.employeeid)]


# In[ ]:


inserts.info()


# In[ ]:


inserts.head()


# In[ ]:


# 3. Get modified rows
modified = changes[changes.employeeid.isin(df1.employeeid)]


# In[ ]:


modified.info()


# In[ ]:


modified.head()


# In[ ]:


for index, row in changes.iterrows():
    cursor.execute(
        "INSERT INTO company_cost (employeeid, productname, cost, totalsales, totalamount, locations) VALUES (%s, %s, %s, %s, %s, %s)",
        (row['employeeid'], row['productname'], row['cost'], row['totalsales'], row['totalamount'], row['locations'])
    )


# In[ ]:


connection.commit()


# In[ ]:


#Checking the tables 


# In[ ]:


query1 = "select * from company_cost"


# In[ ]:


dfa = pd.read_sql(query1, connection)


# In[ ]:


dfa.head()


# In[ ]:


dfa.info()


# In[ ]:


#ID's Implementation
dfa['Unique_ID'] = range(1000, 1000 + len(dfa))


# In[ ]:


dfa.head()


# In[ ]:


for index, row in dfa.iterrows():
    cursor.execute(
        "INSERT INTO company_cost (employeeid, productname, cost, totalsales, totalamount, locations,Unique_ID) VALUES (%s, %s, %s, %s, %s, %s,%s)",
        (row['employeeid'], row['productname'], row['cost'], row['totalsales'], row['totalamount'], row['locations'],row['Unique_ID'])
    )


# In[ ]:


connection.commit()

