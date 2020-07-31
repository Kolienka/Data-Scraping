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

listeCles = ["ville", "lien","Allocataires CAF", "Bénéficiaires du RSA", " - bénéficiaires du RSA majoré", " - bénéficiaires du RSA socle", "Bénéficiaires de l'aide au logement",
" - bénéficiaires de l'APL (aide personnalisée au logement)", " - bénéficiaires de l'ALF (allocation de logement à caractère familial)",
" - bénéficiaires de l'ALS (allocation de logement à caractère social)", " - bénéficiaires de l'Allocation pour une location immobilière",
" - bénéficiaires de l'Allocation pour un achat immobilier", "Bénéficiaires des allocations familiales", " - bénéficiaires du complément familial",
" - bénéficiaires de l'allocation de soutien familial", " - bénéficiaires de l'allocation de rentrée scolaire", "Bénéficiaires de la PAJE",
" - bénéficiaires de l'allocation de base", " - bénéficiaires du complément mode de garde pour une assistante maternelle", " - bénéficiaires du complément de libre choix d'activité (CLCA ou COLCA)",
" - bénéficiaires de la prime naissance ou adoption", "Médecins généralistes", "Masseurs-kinésithérapeutes", "Dentistes", "Infirmiers",
"Spécialistes ORL", "Ophtalmologistes", "Dermatologues", "Sage-femmes", "Pédiatres", "Gynécologues", "Pharmacies", "Urgences", "Ambulances",
"Etablissements de santé de court séjour", "Etablissements de santé de moyen séjour", "Etablissements de santé de long séjour", "Etablissement d'accueil du jeune enfant",
"Maisons de retraite", "Etablissements pour enfants handicapés", "Etablissements pour adultes handicapés"]

dico = {
    **{i : '' for i in listeCles},
    **{"nombre allocataires (" + str(a) + ")" : '' for a in range(2009,2018)},
    **{"nombre RSA (" + str(a) + ")" : '' for a in range(2009,2018)},
    **{"nombre APL (" + str(a) + ")" : '' for a in range(2009,2018)},
    **{"nombre allocations Familiales (" + str(a) + ")" : '' for a in range(2009,2019)},
    }

colonnes = list(dico.keys())

# fonction qui va faire la difference entre les liens scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\santeSocial.csv'):
    tableauSante = pd.read_csv('dataset\\santeSocial.csv', error_bad_lines=False, dtype='unicode')
    colonnes1 = tableauSante['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # Creation de notre csv infos
    tableauSante = DataFrame(columns= colonnes)
    tableauSante.to_csv('dataset\\santeSocial.csv', index=False)
    # Je recupere la liste des liens a scraper
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']
# print(listeLiens)


def parse(lien):
    dico = {
        **{i : '' for i in listeCles},
        **{"nombre allocataires (" + str(a) + ")" : '' for a in range(2009,2018)},
        **{"nombre RSA (" + str(a) + ")" : '' for a in range(2009,2018)},
        **{"nombre APL (" + str(a) + ")" : '' for a in range(2009,2018)},
        **{"nombre allocations Familiales (" + str(a) + ")" : '' for a in range(2009,2019)},
        }
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/sante-social")
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\santeSocial.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            for i in range(len(tables)):
                infos = tables[i].findAll('tr')
                for info in infos[1:]:
                    cle = info.findAll('td')[0].text 
                    valeur = info.findAll('td')[1].text 

                    valeur = ''.join(valeur.split())
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle] = valeur

            divs = soup.findAll('div', class_="hidden marB20")
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Nombre d'allocataires" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nombre allocataires (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nombre allocataires (" + str(annee) + ")"] = ''

                if titre_h2 != None and "Nombre de bénéficiaires du RSA" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nombre RSA (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nombre RSA (" + str(annee) + ")"] = ''

                if titre_h2 != None and "l'aide au logement" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nombre APL (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nombre APL (" + str(annee) + ")"] = ''

                if titre_h2 != None and "allocations familiales" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nombre allocations Familiales (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nombre allocations Familiales (" + str(annee) + ")"] = ''

            writer.writerow(dico)
            print("[sante]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)
        