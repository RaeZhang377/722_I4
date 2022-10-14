#!/usr/bin/env python
# coding: utf-8

# In[82]:


import findspark
findspark.init('/home/ubuntu/spark-3.2.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('722').getOrCreate()


# In[83]:


ghg = spark.read.csv("./AIR_GHG_25072022184805881.csv",inferSchema=True,header=True)
print(ghg)

ghg.count()


# In[84]:


ghg.printSchema()


# In[85]:


ghg.describe().show()


# In[86]:


ghg.head()


# In[87]:


# Let's visualise the columns to help with assembly. 
ghg.columns


# In[88]:


# Import the relevant Python libraries.
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

ValueArr = np.array(ghg.select('Value').collect())
plt.hist(ValueArr)
plt.show()


# In[89]:


CountryArr = np.array(ghg.select('Country').collect())
plt.hist(CountryArr)
plt.show()


# In[90]:


YearArr = np.array(ghg.select('Year').collect())
plt.hist(YearArr)
plt.show()


# In[91]:


ghg = ghg[ghg["Pollutant"] == "Greenhouse gases"]
ghg=ghg[["Year","Country","Value"]]


# In[92]:


# Drops a row if a value from a particular row is missing. Two rows are dropped.
ghg.na.drop(subset="Value").show()
ghg.printSchema()

ghg.count()


# In[93]:


ghg.groupBy('Country').mean().show()


# In[94]:


# Also, it's good practice to use your sales average to fill missing data. 
from pyspark.sql.functions import mean

# Let's collect the average. You'll notice that the collection returns the average in an interesting format.
mean_value = ghg.select(mean(ghg['Value'])).collect()
mean_value


# In[95]:


ghg.filter(ghg['Value'] < 0).select('Country','Year','Value').show()

ghg.filter(ghg['Value'] == 0).select('Country','Year','Value').show()


# In[96]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

ghg_pivot = ghg.groupBy('Country') \
        .pivot('Year') \
        .agg(F.sum('Value')) \
        .fillna(0)

ghg_pivot.show()

ghg_pivot.count()


# In[97]:


Column_2020 = ghg_pivot.select('2020')

Column_2020.show()

Column_2020.count()


# In[99]:


ghg_pivot.dropna(how='any').show( )

ghg_pivot.count()


# In[ ]:




