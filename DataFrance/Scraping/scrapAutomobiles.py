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

colonnes = ['total de voitures', "Ménages sans voiture","Ménages avec une voiture", "Ménages avec deux voitures ou plus", 
"Ménages avec place(s) de stationnement", "Nombre total d'accidents","Nombre de personnes tuées",
"Nombre de personnes indemnes", "Nombre de personnes blessées", " - dont blessés graves", " - dont blessés légers",'ville','lien']

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\auto.csv"):
    # instructions
    tableauInfos = pd.read_csv('dataset\\auto.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauInfos['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # instructions
    tableauInfos = DataFrame(columns = colonnes)
    tableauInfos.to_csv('dataset\\auto.csv', index=False)
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']


def parse(lien):
    dico = {i : '' for i in colonnes}

    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/auto")
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\auto.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            # Nombe de total de voitures
            divs = soup.findAll('div', class_ = 'hidden marB20')
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "ménages avec voiture" in titre_h2.text:
                    try:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        dico['total de voitures'] = float(json_data['series'][0]['data'][-1])
                    except:
                        dico['total de voitures'] = 'NaN'

            tables = soup.findAll('table', class_='odTable odTableAuto')
            
            for info in tables[0].findAll('tr')[1:]:
                cle = info.findAll('td')[0].text
                valeur = info.findAll('td')[1].text
                if valeur != 'nc':
                    dico[cle] = float(''.join(valeur.split()).replace(',','.'))
                else:
                    dico[cle] = 'nc'
            
            if len(tables) >= 2:
                for info in tables[1].findAll('tr')[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text
                    if valeur != 'nc':
                        dico[cle] = float(''.join(valeur.split()).split('(')[0].replace(',','.'))
                    else:
                        dico[cle] = valeur

            writer.writerow(dico)
            print("[Auto]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)
    
