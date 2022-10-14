#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/ubuntu/spark-3.2.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('722').getOrCreate()


# In[23]:


ghg = spark.read.csv("./AIR_GHG_25072022184805881.csv",inferSchema=True,header=True)
print(ghg)


# In[13]:


ghg.printSchema()


# In[15]:


ghg.head()


# In[18]:


# Let's visualise the columns to help with assembly. 
ghg.columns


# In[43]:


# Import the relevant Python libraries.
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

ValueArr = np.array(ghg.select('Value').collect())
plt.hist(ValueArr)
plt.show()


# In[44]:


CountryArr = np.array(ghg.select('Country').collect())
plt.hist(CountryArr)
plt.show()


# In[45]:


YearArr = np.array(ghg.select('Year').collect())
plt.hist(YearArr)
plt.show()


# In[28]:


ghg = ghg[ghg["Pollutant"] == "Greenhouse gases"]
ghg=ghg[["Year","Country","Value"]]


# In[31]:


# Drops a row if a value from a particular row is missing. Two rows are dropped.
ghg.na.drop(subset="Value").show()
ghg.printSchema()


# In[32]:


ghg.groupBy('Country').mean().show()


# In[30]:


# Also, it's good practice to use your sales average to fill missing data. 
from pyspark.sql.functions import mean

# Let's collect the average. You'll notice that the collection returns the average in an interesting format.
mean_value = ghg.select(mean(ghg['Value'])).collect()
mean_value


# In[35]:


ghg.filter("Value <0").select('Country','Year').show()


# In[36]:


ghg.filter("Value =0").select('Country','Year').show()


# In[37]:


ghg = ghg[ghg["Value"] > 0]


# In[22]:


CountryColumn = ghg.select('Country')

CountryColumn.show()


# In[ ]:




