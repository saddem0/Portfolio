import pandas as pd
import mysql.connector
import os

my_path='C:\\Users\\benne\\Desktop\\Mise en situation'
os.chdir(my_path)
excel_file = 'PRIX_BMW.xlsx'
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'toor',
    'database': 'dw_vente_bmw'
}


df1 = pd.read_excel(excel_file, sheet_name=0)
df1["Marque"] = "BMW"

df2 = pd.read_excel(excel_file, sheet_name=1)
df2["Marque"] = "Mini"

df3 = pd.read_excel(excel_file, sheet_name=2)
df3["Marque"] = "Rolls-Royce"

df = pd.concat([df1, df2, df3])
#--------

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

for index, row in df.iterrows():
    query = "INSERT INTO referentiel_prix (prix_min, prix_max, modele,energie,Marque) VALUES (%s, %s, %s, %s, %s)"
    values = (format_numeric_input(row['Prix min']), format_numeric_input(row['Prix max']), row['Modele'], row['Energie'], row['Marque'])
    print(values)
    cursor.execute(query, values)


connection.commit()
cursor.close()
connection.close()

def format_numeric_input(obj,multipli=1):
    if not isinstance(obj, (int, float)):

        obj = str(obj).replace(" ", "").replace("\xa0", "").replace(",", ".").replace("$", "")
        while obj.count('.')>1:
            point_idx = obj.find('.')
            obj = obj[:point_idx] + obj[point_idx+1:]
        if obj.find('.') < len(obj)-3:
            obj=obj.replace(".", "")
    return float(obj)*multipli


