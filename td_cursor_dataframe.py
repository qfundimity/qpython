from pandas import DataFrame 
con = teradatasql.connect(host='172.28.130.20', user='kmania001c', password='EastBrunswick@2022', logmech = "LDAP", encryptdata='true')
#create cursor
crsr = con.cursor()
print('my cursor: ',crsr)
""""crsr.execute('select current_date')
print(crsr.fetchall())
crsr.execute('select * from ndw_ebi_dmr_shared_views.product_dim sample 5')
print(crsr.fetchall())
#above output is not readable and we need to put it in a dataframe
"""
list_cur = list(crsr.execute('select * from ndw_ebi_dmr_shared_views.topology_dim sample 5'))

# Converting cursor to the list of 
# dictionaries
#list_cur = list(crsr)
  
# Converting to the DataFrame
df = DataFrame(list_cur)
#print('Type of df:',type(df))

# change the column names on the data frame

df = df.rename(columns={0:'id',1:'country',2:'division',3:'region',4:'entity'})

# Printing the df to console
#print()
print(df.head())
con.close()
