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


listeCles = ['ville','lien','Salaire moyen des cadres','Salaire moyen des professions intermédiaires',
       'Salaire moyen des employés', 'Salaire moyen des ouvriers','Salaire moyen des femmes', 'Salaire moyen des hommes',
       'Salaire moyen des moins de 26 ans', 'Salaire moyen des 26-49 ans','Salaire moyen des 50 ans et plus',
       'Revenu mensuel moyen par foyer fiscal', 'Nombre de foyers fiscaux',"Nombre moyen d'habitant(s) par foyer"]

dico = {
    **{i : '' for i in listeCles},
    **{str(annee): '' for annee in range(2006,2016)} 
    }

colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\salaires.csv"):
    tableauSalaires = pd.read_csv('dataset\\salaires.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauSalaires['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauSalaires = DataFrame(columns = colonnes)
    tableauSalaires.to_csv('dataset\\salaires.csv', index=False)

    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {
        **{i : '' for i in listeCles},
        **{str(annee): '' for annee in range(2006,2016)} 
        }
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    dico['lien'] = lien

    req = requests.get("http://www.journaldunet.com/business/salaire/" + lien[18:])
    if req.status_code == 200:
        with open('dataset\\salaires.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_= "odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text 
                        valeur = info.findAll('td')[1].text
                        try:
                            if 'foyers' in cle:
                                dico[cle] = float(''.join(valeur.split()).split('f')[0])
                            elif 'Salaire' in cle:
                                dico[cle] = float(''.join(valeur.split()).split('€')[0])
                            elif "d'habitant(s)" in cle:
                                dico[cle] = float(''.join(valeur.split()).split('p')[0].replace(',', '.'))
                            elif "Revenu" in cle:
                                dico[cle] = float(''.join(valeur.split()).split('€')[0])
                            else:
                                dico[cle] = float(''.join(valeur.split()))
                        except:
                            dico[cle] = 'nc'

            divs = soup.findAll('div', class_ = 'hidden marB20')
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Evolution des revenus" in titre_h2.text:
                    js_script = div.find('script').string
                    json_data = json.loads(js_script)
                    annees = json_data['xAxis']['categories']
                    salaires = json_data['series'][0]['data']
                    for annee, salaire in zip(annees, salaires):
                        dico[str(annee)] = salaire

            time.sleep(1)
            writer.writerow(dico)
            print("[salaires]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)