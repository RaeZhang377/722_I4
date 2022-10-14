#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/ubuntu/spark-3.2.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('722').getOrCreate()


# In[10]:


ghg = spark.read.csv("./AIR_GHG_25072022184805881.csv",inferSchema=True,header=True)


# In[11]:


print(ghg)


# In[13]:


ghg.printSchema()


# In[15]:


ghg.head()


# In[16]:


# Import VectorAssembler and Vectors
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


# In[18]:


# Let's visualise the columns to help with assembly. 
ghg.columns


# In[ ]:




