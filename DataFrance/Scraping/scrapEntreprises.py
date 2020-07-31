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


listeCles = ["Nombre d'entreprises", "- dont commerces et services aux particuliers", "Entreprises créées", "Commerces",
"Services aux particuliers", "Services publics", "Epiceries", "Boulangeries", "Boucheries, charcuteries",
"Librairies, papeteries, journaux", "Drogueries et quincalleries", "Banques", "Bureaux de Poste",
"Garages, réparation automobile", "Electriciens", "Grandes surfaces", "Commerces spécialisés alimentaires", "Commerces spécialisés non alimentaires",
"Services généraux", "Services automobiles", "Services du bâtiment", "Autres services", 'ville','lien']

dico = {
    **{i : '' for i in listeCles},
    **{str(annee) + " (nbre de creations)": '' for annee in range(2005,2016)},
    **{str(annee) + " (nbre d'entreprises)": '' for annee in range(2005,2016) }
}

colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\entreprises.csv"):
    tableauEntreprises = pd.read_csv('dataset\\entreprises.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauEntreprises['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauEntreprises = DataFrame(columns = colonnes)
    tableauEntreprises.to_csv('dataset\\entreprises.csv', index=False)

    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {
            **{i : '' for i in listeCles},
            **{str(annee) + " (nbre de creations)": '' for annee in range(2005,2016)},
            **{str(annee) + " (nbre d'entreprises)": '' for annee in range(2005,2016) }
        }
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/entreprises")
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\entreprises.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text 
                        valeur = info.findAll('td')[1].text
                        try:
                            dico[cle] = float(''.join(valeur.split()))
                        except:
                            dico[cle] = 'nc'

            # Evolution du nombre et de creations d'entreprises
            divs = soup.findAll('div', class_ = 'marB20')
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Nombre d'entreprises" in titre_h2.text:
                    js_script = div.find('script').string
                    json_data_en = json.loads(js_script)
                    annees = json_data_en['xAxis']['categories']
                    entreprises = json_data_en['series'][0]['data']
                    for annee, entreprise in zip(annees, entreprises):
                        dico[str(annee) + " (nbre d'entreprises)"] = float(entreprise)

                if titre_h2 != None and "Créations d'entreprises" in titre_h2.text:
                    js_script = div.find('script').string
                    json_data_en = json.loads(js_script)
                    annees = json_data_en['xAxis']['categories']
                    creations = json_data_en['series'][0]['data']
                    for annee, creation in zip(annees, creations):
                        dico[str(annee) + " (nbre de creations)"] = float(creation)

            writer.writerow(dico)
            print("[entreprises]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)