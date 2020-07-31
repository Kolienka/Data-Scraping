from bs4 import BeautifulSoup as bs 
from multiprocessing import Pool
import pandas as pd 
from pandas import DataFrame 
from pprint import pprint
import json
import requests
import time
import csv
import os
import re

colonnes = ['ville', 'lien', 'prix_m2', 'Nombre de logements',"Nombre moyen d'habitant(s) par logement", 'Résidences principales',
            'Résidences secondaires', 'Logements vacants', 'Maisons','Appartements', 'Autres types de logements', 'Propriétaires',
            'Locataires', '- dont locataires HLM',"Locataires hébergés à titre gratuit", 'Studios', '2 pièces',
            '3 pièces', '4 pièces', '5 pièces et plus']
# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\immobilier.csv"):
    tableauImmo = pd.read_csv('dataset\\immobilier.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauImmo['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauImmo = DataFrame(columns = colonnes)
    tableauImmo.to_csv('dataset\\immobilier.csv', index=False)
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {i : '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/immobilier")
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\immobilier.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            try:
                js_script = soup.findAll('script')[6].string
                json_prix = json.loads(js_script)
                try:
                    dico['prix_m2'] = float(json_prix['series'][0]['data'][0])
                except:
                    dico['prix_m2'] = json_prix['series'][0]['data'][0]
            except:
                dico['prix_m2'] = 'nc'

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text
                        valeur = info.findAll('td')[1].text
                        if "Locataires hébergés" in cle:
                            try:
                                dico["Locataires hébergés à titre gratuit"] = float(''.join(valeur.split()).replace(',','.'))
                            except:
                                dico["Locataires hébergés à titre gratuit"] = valeur
                        elif "5 pièces" in cle:
                            try:
                                dico["5 pièces et plus"] = float(''.join(valeur.split()).replace(',','.'))
                            except:
                                dico["5 pièces et plus"] = valeur
                        else:
                            try:
                                dico[cle] = float(''.join(valeur.split()).replace(',','.'))
                            except:
                                dico[cle] = valeur
            
            writer.writerow(dico)
            print("[immo]", lien)

if __name__ == "__main__":
    with Pool(20) as p:
        p.map(parse, listeLiens)
