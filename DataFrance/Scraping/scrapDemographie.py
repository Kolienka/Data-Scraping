import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs 
import pandas as pd 
from pandas import DataFrame
import csv
from pprint import pprint
import os
import time

colonnes = ["ville","lien","Population","Densité de population","Nombre de ménages","Habitants par ménage",
"Nombre de familles","Naissances","Décès","Solde naturel","Hommes","Femmes","Moins de 15 ans","15 - 29 ans","30 - 44 ans",
"45 - 59 ans","60 - 74 ans","75 ans et plus","Familles monoparentales","Couples sans enfant","Couples avec enfant",
"Familles sans enfant","Familles avec un enfant","Familles avec deux enfants","Familles avec trois enfants",
"Familles avec quatre enfants ou plus","Personnes célibataires","Personnes mariées","Personnes divorcées",
"Personnes veuves","Population étrangère","Hommes étrangers","Femmes étrangères","Moins de 15 ans étrangers",
"15-24 ans étrangers","25-54 ans étrangers","55 ans et plus étrangers","Population immigrée","Hommes immigrés",
"Femmes immigrées","Moins de 15 ans immigrés","15-24 ans immigrés","25-54 ans immigrés","55 ans et plus immigrés"]

url = '/management/ville/aast/ville-64001/demographie'

req = requests.get("http://www.journaldunet.com" + url)
contenu = req.content
soup = bs (contenu,"html.parser")

tables = soup.findAll('table', class_ = "odTable odTableAuto")

dico = {i : '' for i in colonnes}

for i in range(len(tables)):
    infos = tables[i].findAll('tr')
    for info in infos[1:]:
        cle = info.findAll('td')[0].text.split('(')[0].strip()
        valeur = info.findAll('td')[1].text.split('h')[0].strip().replace(',','.')
        print(cle,valeur)