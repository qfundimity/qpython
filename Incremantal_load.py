import teradatasql
from pandas import DataFrame
import pandas as pd
con = teradatasql.connect(host='172.28.130.20', user='kmania001c', password='EastBrunswick@2022', logmech = "LDAP", encryptdata='true')
#create cursor
crsr = con.cursor()
print('my cursor: ',crsr)

list_cur = list(crsr.execute('select topology_key,country_nm,division_nm,region_nm,entity_cd from ndw_ebi_dmr_shared_views.topology_dim  sample 5'))

# Converting List to the DataFrame
df = DataFrame(list_cur)
# Working with dataframes to see clean(table) format data 
# change the column names on the data frame
df = df.rename(columns={0:'id',1:'country',2:'division',3:'region',4:'entity'})
df = df.loc[:,['id','country','division','region','entity']]
print(df.head())
print('cursor executmany method will need data in form or tuples so use the list format of the data ')
print(list_cur)
  
#Day 1
# Cursor need a data in the form of list, so using the initial format where SQL data was read frm db and converted to list 
ins_sql = "insert into ndw_temp.qregion(id,country,division,region, entity ) values(?,?,?,?,?)"
crsr.executemany(ins_sql, list_cur)
con.commit()
print(crsr.rowcount, "Record inserted successfully into NDW temp table")

#Day2
sel_incr = "select topology_key,country_nm,division_nm,region_nm,entity_cd from " \
"ndw_ebi_dmr_shared_views.topology_dim " \
"where topology_key not in  "\
" ( select id from ndw_temp.qregion ) order by 1 sample 5"

ins_incr = "insert into ndw_temp.qregion(id,country,division,region, entity ) values(?,?,?,?,?)"
sel_incrl = list(crsr.execute(sel_incr))
#print("Incremantal data :",crsr.fetchall())

crsr.executemany(ins_incr, sel_incrl)
con.commit()
print(crsr.rowcount, "Record inserted successfully into NDW temp table")

#PRINT OUTPUT IN A LIST FORMAT
crsr.execute('select * from ndw_temp.qregion')
print('Output in List formart -- each row is a tuple')
print(crsr.fetchall())

#PRINT THE OUTPUT IN A TABULATION FORMAT FOR THE DATA IN THE TARGET TABLE
dfinal = DataFrame(crsr.execute('select * from ndw_temp.qregion order by id'))
print('Output in Tabulation format using dataframes')
dfinal = dfinal.rename(columns={0:'id',1:'country',2:'division',3:'region',4:'entity'})
print(dfinal)
#con.close()
