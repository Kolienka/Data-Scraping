import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs 
import pandas as pd 
from pandas import DataFrame
import csv
from pprint import pprint
import os
import time

colonnes = ['ville','lien', "Code Insee", "Région", "Département", "Etablissement public de coopération intercommunale (EPCI)",
"Code postal (CP)", "Nom des habitants", "Population (2017)",
"Population : rang national (2017)", "Densité de population (2017)", "Taux de chômage (2016)",
"Pavillon bleu", "Ville d'art et d'histoire", "Ville fleurie", "Ville internet", "Superficie (surface)",
"Altitude min.", "Altitude max.", "Latitude", "Longitude"]

#fonction qui calcule la différence entre les liens scrappés et non scrappés
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\infos.csv'):
    tableauInfos = pd.read_csv('dataset\\infos.csv', error_bad_lines=False)
    colonnes1 = tableauInfos['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1,colonnes2)
else:
    # Création de notre csv infos
    tableauInfos = DataFrame(columns= colonnes)
    tableauInfos.to_csv('dataset\\infos.csv', index=False)
    # Je récupère la liste des liens à scraper
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if lien[:11] == '/management']

def parse(lien):
    # Initialisation d'un dictionnaire
    dico = {i : '' for i in colonnes}
    
    req = requests.get("http://www.journaldunet.com" + lien)
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\infos.csv', 'a', encoding ='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            dico['lien'] = lien
            dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

            tables = soup.findAll('table', class_ = 'odTable odTableAuto')

            for i in range(len(tables)):
                tousLesTr = tables[i].findAll('tr')
                for tr in tousLesTr[1:]:
                    cle = tr.findAll('td')[0].text
                    valeur = tr.findAll('td')[1].text

                    if "Nom des habitants" in cle:
                        dico["Nom des habitants"] = str(valeur)
                    elif "Taux de chômage" in cle:
                        dico["Taux de chômage (2016)"] = str(valeur)
                    else:
                        dico[cle] = valeur

            writer.writerow(dico)
            print(lien)
       
if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)