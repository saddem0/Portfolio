import pandas as pd
import mysql.connector
import os

my_path='C:\\Users\\benne\\Desktop\\Mise en situation'
os.chdir(my_path)
excel_file = 'Prix.xlsx'
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'toor',
    'database': 'dw_vente_steallantis'
}
dollar_euro_ratio = 0.9243

df1 = pd.read_excel(excel_file, sheet_name='Prix Stellantis')

df2 = pd.read_excel(excel_file, sheet_name='Prix Ram')
df2["Marque"] = "Ram"



df = pd.concat([df1, df2 ])
df.loc[df['Boite de vitesse'] == 'Thermique','Boite de vitesse']='Manuelle'
df.loc[df['Boite de vitesse'] == 'Automatic','Boite de vitesse']='Automatique'
#--------Automatic

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

for index, row in df.iterrows():
    query = "INSERT INTO referentiel_prix (prix_min, prix_max, modele,energie,marque,boite_vitesse) VALUES (%s, %s, %s, %s, %s, %s)"

    if isinstance(row['Prix min'], (int, float)) or row['Prix min'].find('$') == -1:
        prix_min = format_numeric_input(row['Prix min'])
    else:
        prix_min = format_numeric_input(row['Prix min'],dollar_euro_ratio)
    if isinstance(row['Prix max'], (int, float)) or row['Prix max'].find('$') == -1:
        prix_max = format_numeric_input(row['Prix max'])
    else:
        prix_max = format_numeric_input(row['Prix max'],dollar_euro_ratio)

    values = (prix_min, prix_max, row['Modele'], row['Energie'], row['Marque'],row['Boite de vitesse'])
    cursor.execute(query, values)


connection.commit()
cursor.close()
connection.close()

def format_numeric_input(obj,multipli=1):
    print(obj,type(obj))
    if not isinstance(obj, (int, float)):

        obj = str(obj).replace(" ", "").replace("\xa0", "").replace(",", ".").replace("$", "")
        while obj.count('.')>1:
            point_idx = obj.find('.')
            obj = obj[:point_idx] + obj[point_idx+1:]
        if obj.find('.') < len(obj)-3:
            obj=obj.replace(".", "")
    return float(obj)*multipli


