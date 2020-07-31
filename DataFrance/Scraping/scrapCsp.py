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

colonnes = ["ville", "lien", "Agriculteurs exploitants", "Artisans, commerçants, chefs d'entreprise",
    "Cadres et professions intellectuelles supérieures", "Professions intermédiaires","Employés","Ouvriers",
    "Aucun diplôme", "CAP / BEP ", "Baccalauréat / brevet professionnel", "Diplôme de l'enseignement supérieur",
    "Aucun diplôme (%) hommes", "Aucun diplôme (%) femmes", "CAP / BEP  (%) hommes", "CAP / BEP  (%) femmes",
    "Baccalauréat / brevet professionnel (%) hommes", "Baccalauréat / brevet professionnel (%) femmes",
    "Diplôme de l'enseignement supérieur (%) hommes", "Diplôme de l'enseignement supérieur (%) femmes"]

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\csp.csv"):
    tableauCsp = pd.read_csv('dataset\\csp.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauCsp['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauCsp = DataFrame(columns = colonnes)
    tableauCsp.to_csv('dataset\\csp.csv', index=False)

    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {i : '' for i in colonnes}
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    dico['lien'] = lien

    req = requests.get("http://www.journaldunet.com" + lien + "/csp-diplomes")
    if req.status_code == 200:
        with open('dataset\\csp.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_= "odTable odTableAuto")
            for i in range(len(tables)-1):
                for table in tables[i].findAll('tr')[1:]:
                    cle = table.findAll('td')[0].text
                    valeur = table.findAll('td')[1].text
                    try:
                        dico[cle] = float(''.join(valeur.split()))
                    except:
                        dico[cle] = ''
                        
            for table in tables[-1].findAll('tr')[1:]:
                cle = table.findAll('td')[0].text
                valeurh = table.findAll('td')[1].text
                valeurf = table.findAll('td')[3].text
                try:
                    dico[cle + " (%) hommes"] = float(valeurh.split('%')[0].replace(',','.'))
                    dico[cle + " (%) femmes"] = float(valeurf.split('%')[0].replace(',','.'))
                except:
                    dico[cle + " (%) hommes"] = ''
                    dico[cle + " (%) femmes"] = ''
            

            time.sleep(1)
            writer.writerow(dico)
            print("[csp]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)