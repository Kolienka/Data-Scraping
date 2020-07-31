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


colonnes = ['ville', 'lien', 'Actifs en emploi',
       'Aides familiaux', 'Autres personnes sans activité de 15-64 ans', 'CDD',
       'CDI et fonction publique', 'Chômeurs', 'Emplois aidés', 'Employeurs',
       'Femmes à temps partiel', 'Hommes à temps partiel', 'Inactifs',
       'Indépendants', 'Intérimaires', 'Les 15 à 24 ans à temps partiel',
       'Les 25 à 54 ans à temps partiel', 'Les 55 à 64 ans à temps partiel',
       'Non-salariés', 'Part des actifs 15-24 ans (%)',
       'Part des actifs 25-54 ans (%)', 'Part des actifs 55-64 ans (%)',
       'Part des actifs femmes (%)', 'Part des actifs hommes (%)',
       'Retraités et pré-retraités de 15-64 ans', 'Salariés',
       'Salariés à temps partiel', 'Stages et apprentissages',
       'Stagiaires et étudiants de 15-64 ans', "Taux d'activité femmes (%)",
       "Taux d'activité hommes (%)", "Taux d'emploi 15-24 ans (%)",
       "Taux d'emploi 25-54 ans (%)", "Taux d'emploi 55-64 ans (%)",
       "Taux d'emploi femmes (%)", "Taux d'emploi hommes (%)",
       'Taux de chômage 15-24 ans (%)', 'Taux de chômage 25-54 ans (%)',
       'Taux de chômage 55-64 ans (%)', 'Taux de chômage femmes (%)',
       'Taux de chômage hommes (%)']

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\emploi.csv"):
    tableauEmploi = pd.read_csv('dataset\\emploi.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauEmploi['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauEmploi = DataFrame(columns = colonnes)
    tableauEmploi.to_csv('dataset\\emploi.csv', index=False)

    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {i : '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/emploi")
    # time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\emploi.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            if len(tables) > 0:
                ## collecte du tableau 1, 4, 5, 6, 7
                for i in [0, 3, 4, 5, 6]:
                    for table in tables[i].findAll('tr')[1:]:
                        cle = table.findAll('td')[0].text
                        valeur = table.findAll('td')[1].text
                        try:
                            dico[cle] = float(''.join(valeur.split()))
                        except:
                            dico[cle] = 'nc'

                ## Collecte du tableau 2
                for table in tables[1].findAll('tr')[1:]:
                    cle = table.findAll('td')[0].text
                    valeurh = table.findAll('td')[1].text
                    valeurf = table.findAll('td')[3].text
                    try:
                        dico[cle + " hommes (%)"] = float(valeurh.split()[0].replace(',','.'))
                        dico[cle + " femmes (%)"] = float(valeurf.split()[0].replace(',','.'))
                    except:
                        dico[cle + " hommes (%)"] = 'nc'
                        dico[cle + " femmes (%)"] = 'nc'

                ## Collecte du tableau 3
                for table in tables[2].findAll('tr')[1:]:
                    cle = table.findAll('td')[0].text
                    valeur15_24 = table.findAll('td')[1].text
                    valeur25_54 = table.findAll('td')[2].text
                    valeur55_64 = table.findAll('td')[3].text
                    try:
                        dico[cle + ' 15-24 ans (%)'] = float(valeur15_24.split('%')[0].replace(',','.'))
                        dico[cle + ' 25-54 ans (%)'] = float(valeur25_54.split('%')[0].replace(',','.'))
                        dico[cle + ' 55-64 ans (%)'] = float(valeur55_64.split('%')[0].replace(',','.'))
                    except:
                        dico[cle + ' 15-24 ans (%)'] = 'nc'
                        dico[cle + ' 25-54 ans (%)'] = 'nc'
                        dico[cle + ' 55-64 ans (%)'] = 'nc'

            time.sleep(2)
            writer.writerow(dico)
            print("[emploi]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)