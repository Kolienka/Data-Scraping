import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs 
import pandas as pd 
from pandas import DataFrame
import csv
from pprint import pprint
import json
import os
import time
import re


colonnes = ['ville','lien','Nathalie LOISEAU','Jordan BARDELLA','François-Xavier BELLAMY','Yannick JADOT','Benoît HAMON','Raphaël GLUCKSMANN',
'Manon AUBRY','Jean-Christophe LAGARDE','Hélène THOUY','Dominique BOURG','Nicolas DUPONT-AIGNAN','Florian PHILIPPOT',
'Olivier BIDOU','Francis LALANNE','François ASSELINEAU','Christophe CHALENÇON','Pierre DIEUMEGARD','Antonio SANCHEZ',
'Cathy Denise Ginette CORBET','Christian Luc PERSON','Nathalie ARTHAUD','Nathalie TOMASINI','Ian BROSSAT',
'Robert DE PREVOISIN','Thérèse DELFEL','Sophie CAILLAUD','Gilles HELGEN','Yves GERNIGON','Vincent VAUCLIN',
'Audric ALEXANDRE','Hamada TRAORÉ','Florie MARIE','Renaud CAMUS','Nagib AZERGUI','Taux de participation','Taux d\'abstention',
'Votes blancs (en pourcentage des votes exprimés)','Votes nuls (en pourcentage des votes exprimés)','Nombre de votants']

def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\elections.csv'):
    tableauElections = pd.read_csv('dataset\\elections.csv', error_bad_lines=False, dtype='unicode')
    colonnes1 = tableauElections['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # Creation de notre csv infos
    tableauElections = DataFrame(columns= colonnes)
    tableauElections.to_csv('dataset\\elections.csv', index=False)
    # Je recupere la liste des liens a scraper
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

def parse(lien):
    dico = {i : '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    req = requests.get('https://election-europeenne.linternaute.com/resultats/' + lien[18:])
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\elections.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            divs = soup.findAll('div', class_ = "marB20")

            tableau = divs[3]

            candidats = tableau.findAll('tr',class_ = re.compile('color'))

            for candidat in candidats:
                cle = candidat.find('strong').text
                valeur = candidat.findAll('td')[1].text.replace(',','.').replace('%','')
                dico[cle] = valeur

            tables = tableau.findAll('table')
            if len(tables) == 2:
                for info in tables[1].findAll('tr')[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text.replace(',','.').replace('%','').replace(' ','')
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle] = valeur
                        
            writer.writerow(dico)
            print("[Elections]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)
    