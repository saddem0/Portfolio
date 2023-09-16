import pandas as pd
import mysql.connector
import os

my_path='C:\\Users\\benne\\Desktop\\Mise en situation'
os.chdir(my_path)
excel_file = 'BMW_report_2022.xlsx'
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'toor',
    'database': 'dw_vente_bmw'
}


df1 = pd.read_excel(excel_file, sheet_name=1)
df1.drop(["Unnamed: 0", "Change in %"], axis=1, inplace=True)
df1.columns = "Ventes" + df1.columns
df1.rename(columns={"VentesUnnamed: 1": "Marque"}, inplace=True)
df1 = df1.loc[4:6]
df1 = pd.wide_to_long(df1, ["Ventes"], i = "Marque", j = "Année")
df1.rename(index={"VentesMarque": "Marque"}, inplace=True)
df1["Ventes"] = df1["Ventes"].str.replace(",", "").astype("float64")
df1.reset_index(inplace = True)

annees= pd.DataFrame(df1['Année'].unique())
annees['id'] = annees.index+1
annees.rename(columns={0: "Année"}, inplace=True)

marque= pd.DataFrame(df1.iloc[:,0].unique())
marque['id'] = marque.index+1
marque.rename(columns={0: "Marque"}, inplace=True)

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

for index, row in annees.iterrows():
    query = "INSERT INTO dim_annee (PK_Dim_Date,annee) VALUES (%s,%s)"
    values = (row['id'].item(), row['Année'].item())
    cursor.execute(query, values)

for index, row in marque.iterrows():
    query = "INSERT INTO dim_marque (iddim_marque,marque) VALUES (%s, %s)"
    values = (row['id'], row['Marque'])
    cursor.execute(query, values)
connection.commit()

for index, row in df1.iterrows():
    fk_annee = annees[annees['Année']==row['Année']]['id'].iloc[0]
    fk_marque = marque[marque['Marque']==row['Marque']]['id'].iloc[0]
    query = "INSERT INTO tf_ventes_marque_bmw (Qtt_vendu,fk_annee,fk_marque) VALUES (%s,%s,%s)"
    values = (row['Ventes'], fk_annee.item(),fk_marque.item())
    cursor.execute(query, values)




connection.commit()

#-----------------Zone geo

df2 = pd.read_excel(excel_file, sheet_name=2)
df2 = pd.concat([df2[:1],df2[3:4],df2[5:8]])
df2.rename(columns={"in 1,000 units": "Region"}, inplace=True)
df2['Region'][6]=df2['Unnamed: 1'][6].replace('thereof ','')
df2.drop(["Unnamed: 1"], axis=1, inplace=True)
colomn_annee = ['2022','2021','2020','2019','2018']
df2[colomn_annee] = df2[colomn_annee].applymap(lambda x: float(x.replace(',','')))
df2.set_index(df2['Region'],inplace=True)
df2.drop(["Region"], axis=1, inplace=True)
df2.loc['Asia'] = df2.loc['Asia'] - df2.loc['China']
df2.columns = 'Ventes' + df2.columns
df2.reset_index(inplace=True)
df2 = pd.wide_to_long(df2, ["Ventes"], i = "Region", j = "Année")
df2.reset_index(inplace = True)
df2['Ventes'] = df2['Ventes']*1000
print(df2)


region = pd.DataFrame(df2.iloc[:,0].unique())
region['id'] = region.index+1
region.rename(columns={0: "Région"}, inplace=True)

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

for index, row in region.iterrows():
    query = "INSERT INTO dim_zone_geo (iddim_zone_geo, zone) VALUES (%s, %s)"
    values = (row['id'], row['Région'])
    cursor.execute(query, values)
connection.commit()

for index, row in df2.iterrows():
    fk_annee = annees[annees['Année']==row['Année']]['id'].iloc[0]
    fk_region = region[region['Région']==row['Region']]['id'].iloc[0]
    query = "INSERT INTO tf_ventes_zone_geo_bmw (Qtt_vendu,fk_annee,fk_zone_geo) VALUES (%s,%s,%s)"
    values = (row['Ventes'], fk_annee.item(), fk_region.item())
    cursor.execute(query, values)


connection.commit()


cursor.close()
connection.close()