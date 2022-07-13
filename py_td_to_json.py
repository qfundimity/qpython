# -*- coding: utf-8 -*-
"""py_TD_to_JSON.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1dq4BMYI_s2Hyknj5FOVehA_kBciLEW7H
"""

import teradatasql
import pandas as pd
import json

query = 'select * from ndw_ebi_dmr_shared_views.address_dim sample 50'

with teradatasql.connect(host='172.28.130.20', user='kmania001c', password='EastBrunswick@2022', logmech = "LDAP", encryptdata='true') as connect:  
  data1 = pd.read_sql(query, connect)
  print(data1.head())

#write data to json file
#Day1 - load first 10 records

d3 = data2.head(10).to_dict()
j1 = json.dumps(d3,indent=2, sort_keys= True)
print(j1)

with open('addr_dim.json','w', encoding='utf-8') as outfile:
   json.dump(j1,outfile,ensure_ascii=False, sort_keys=True)
print('json file is ready')

#write data to json file  ---> Incremental Data
# rows 11 to 20, and two columns
#Day2 - load first 10 records
import json
data2 = data1.loc[11:20,['HOUSE_NUM','DMA_NM']]
print(data2)

d3 = data2.head(10).to_dict()
j1 = json.dumps(d3,indent=2, sort_keys= True)
print(j1)

#incremental data 
with open('addr_dim.json','a', encoding='utf-8') as outfile:
   json.dump(j1,outfile,ensure_ascii=False, sort_keys=True)
print('json file is appended')
