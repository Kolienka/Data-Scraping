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

listeCles = ["ville","lien","Population","Densité de population","Nombre de ménages","Habitants par ménage",
"Nombre de familles","Naissances","Décès","Solde naturel","Hommes","Femmes","Moins de 15 ans","15 - 29 ans","30 - 44 ans",
"45 - 59 ans","60 - 74 ans","75 ans et plus","Familles monoparentales","Couples sans enfant","Couples avec enfant",
"Familles sans enfant","Familles avec un enfant","Familles avec deux enfants","Familles avec trois enfants",
"Familles avec quatre enfants ou plus","Personnes célibataires","Personnes mariées","Personnes divorcées",
"Personnes veuves","Population étrangère","Hommes étrangers","Femmes étrangères","Moins de 15 ans étrangers",
"15-24 ans étrangers","25-54 ans étrangers","55 ans et plus étrangers","Population immigrée","Hommes immigrés",
"Femmes immigrées","Moins de 15 ans immigrés","15-24 ans immigrés","25-54 ans immigrés","55 ans et plus immigrés"]

dico = {
    **{i : '' for i in listeCles},
    **{"nombre habitants (" + str(a)+ ")" : '' for a in range(2006,2018)},
    **{"nombre de naissances (" + str(a)+ ")" : '' for a in range(1999,2019)},
    **{"nombre de décès (" + str(a)+ ")" : '' for a in range(1999,2019)},
    **{"nombre d'étrangers (" + str(a)+ ")" : '' for a in range(2006,2017)},
    **{"nombre d'immigrés (" + str(a)+ ")" : '' for a in range(2006,2017)},
    }

colonnes = list(dico.keys())

#fonction qui calcule la différence entre les liens scrappés et non scrappés
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\demographie.csv'):
    tableauDemographie = pd.read_csv('dataset\\demographie.csv', error_bad_lines=False)
    colonnes1 = tableauDemographie['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1,colonnes2)
else:
    # Création de notre csv démographie
    tableauDemographie = DataFrame(columns= colonnes)
    tableauDemographie.to_csv('dataset\\demographie.csv', index=False)
    # Je récupère la liste des liens à scraper
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if lien[:11] == '/management']

def parse(url):
    # Initialisation d'un dictionnaire
    dico = {
        **{i : '' for i in listeCles},
        **{"nombre habitants (" + str(a)+ ")" : '' for a in range(2006,2016)},
        **{"nombre de naissances (" + str(a)+ ")" : '' for a in range(1999,2019)},
        **{"nombre de décès (" + str(a)+ ")" : '' for a in range(1999,2019)},
        **{"nombre d'étrangers (" + str(a)+ ")" : '' for a in range(2006,2017)},
        **{"nombre d'immigrés (" + str(a)+ ")" : '' for a in range(2006,2017)},
    }

    dico['lien'] = url
    dico['ville'] = tableauLiens[tableauLiens['lien'] == url]['ville'].iloc[0]
    
    req = requests.get("http://www.journaldunet.com" + url + "/demographie")
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\Demographie.csv', 'a', encoding ='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            for i in range(len(tables)):
                infos = tables[i].findAll('tr')
                for info in infos[1:]:
                    cle = info.findAll('td')[0].text.split('(')[0].strip()
                    valeur = info.findAll('td')[1].text.split('h')[0].strip().replace(',','.')
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle] = valeur
                    

            divs = soup.findAll('div', class_ = "hidden marB20")
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Nombre d'habitants" in titre_h2.text:
                    js_script = div.find('script').string
                    json_data = json.loads(js_script)
                    annees = json_data['xAxis']['categories']
                    donnes = json_data['series'][0]['data']
                    
                    for annee, donne in zip(annees,donnes):
                        try:
                            dico["nombre habitants (" + str(annee)+ ")"] = float (donne)
                        except:
                            dico["nombre habitants (" + str(annee)+ ")"] = ''

                if titre_h2 != None and "Naissances et décès" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        if len(json_data['series'][0])!=0:
                            naissances = json_data['series'][0]['data']
                            deces = json_data['series'][1]['data']
                            for annee, naissance, mort in zip(annees,naissances,deces):
                                try:
                                    dico["nombre de naissances (" + str(annee)+ ")"] = float (naissance)
                                    dico["nombre de décès (" + str(annee)+ ")"] = float (mort)
                                except:
                                    dico["nombre de naissances (" + str(annee)+ ")"] = ''
                                    dico["nombre de décès (" + str(annee)+ ")"] = ''
                        else:
                            dico["nombre de naissances (" + str(annee)+ ")"] = ''
                            dico["nombre de décès (" + str(annee)+ ")"] = ''

                if titre_h2 != None and "Nombre d'étrangers" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        etrangers = json_data['series'][0]['data']

                        for annee,etranger in zip(annees,etrangers):
                            try:
                                dico["nombre d'étrangers (" + str(annee)+ ")"] = float (etranger)
                            except:
                                dico["nombre d'étrangers (" + str(annee)+ ")"] = '' 

                if titre_h2 != None and "Nombre d'immigrés" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        immigres = json_data['series'][0]['data']

                        for annee,immigre in zip(annees,immigres):
                            try:
                                dico["nombre d'immigrés (" + str(annee)+ ")"] = float (immigre)
                            except:
                                dico["nombre d'immigrés (" + str(annee)+ ")"] = '' 

            writer.writerow(dico)
            print("[demographie]",url)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse,listeLiens)
                    