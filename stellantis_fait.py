import pandas as pd
import mysql.connector
import os

my_path='C:\\Users\\benne\\Desktop\\Mise en situation'
os.chdir(my_path)
excel_file = 'Classeur_2(4) (1).xlsx'
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'toor',
    'database': 'dw_vente_steallantis'
}

#-----------------Zone geo

df2 = pd.read_excel(excel_file, sheet_name=1)
df2 = df2[0:10]
df2.rename(columns={"Unités de ventes par zone géographique (milliers d'unités)": "Region"}, inplace=True)
df2.set_index(df2['Region'],inplace=True)
df2.drop(["*Brésil seul","*Argentine seule","*Autres Amérique du Sud","Total"], axis=0, inplace=True)
df2.drop(["Region"], axis=1, inplace=True)
colomn_annee = ['2022','2021','2020']
df2.loc['Americas'] = df2.loc['Amérique du Nord'] + df2.loc['Amérique du Sud']
df2.loc['Asia'] = df2.loc['Chine, Inde, Asie, Pacifique'] - df2.loc['*Chine seule']
df2.drop(["Amérique du Nord","Amérique du Sud","Chine, Inde, Asie, Pacifique"], axis=0, inplace=True)
df2.rename(index={'*Chine seule': 'China'}, inplace=True)

df2.columns = 'Ventes' + df2.columns.map(lambda x:str(x))
df2.reset_index(inplace=True)
df2 = pd.wide_to_long(df2, ["Ventes"], i = "Region", j = "Année")
df2.reset_index(inplace = True)
df2['Ventes'] = df2['Ventes'] *1000
print(df2)





region = pd.DataFrame(df2.iloc[:,0].unique())
region['id'] = region.index+1
region.rename(columns={0: "Région"}, inplace=True)


annees= pd.DataFrame(df2['Année'].unique())
annees['id'] = annees.index+1
annees.rename(columns={0: "Année"}, inplace=True)


connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()


for index, row in annees.iterrows():
    query = "INSERT INTO dim_annee (PK_Dim_Date,annee) VALUES (%s,%s)"
    values = (row['id'].item(), row['Année'].item())
    cursor.execute(query, values)



for index, row in region.iterrows():
    query = "INSERT INTO dim_zone_geo (iddim_zone_geo, zone) VALUES (%s, %s)"
    values = (row['id'], row['Région'])
    cursor.execute(query, values)
connection.commit()

for index, row in df2.iterrows():
    fk_annee = annees[annees['Année']==row['Année']]['id'].iloc[0]
    fk_region = region[region['Région']==row['Region']]['id'].iloc[0]
    query = "INSERT INTO tf_ventes_zone_geo_stellantis (Qtt_vendu,fk_annee,fk_zone_geo) VALUES (%s,%s,%s)"
    values = (row['Ventes'], fk_annee.item(), fk_region.item())
    cursor.execute(query, values)


connection.commit()


cursor.close()
connection.close()