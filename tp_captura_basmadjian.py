#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 22:12:44 2024

@author: martin
"""

#%% Importar las librerías necesarias
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import re
import json
#%% Conectar a Mongo Atlas

# Cargar la configuración desde el archivo JSON
with open("config.json", "r") as file:
    config = json.load(file)

# Obtener los datos del archivo JSON
username = config['mongodb']['username']
password = config['mongodb']['password']
cluster_url = config['mongodb']['cluster_url']
database_name = config['mongodb']['database_name']
collection_name = config['mongodb']['collection_name']

# Construir el URI de conexión
uri = f'mongodb+srv://{username}:{password}@{cluster_url}/{database_name}?retryWrites=true&w=majority'

try:
    client = MongoClient(uri)
    
    client.admin.command('ping')
    print('Conexión exitosa con MongoDB Atlas')

    db = client[database_name]  
    print(f'Base de datos {database_name} creada o seleccionada exitosamente')

    collection = db[collection_name] 
    print(f'Colección {collection_name} creada o seleccionada exitosamente')

except ConnectionFailure as e:
    print(f'No se pudo conectar a MongoDB Atlas: {e}')
#%% solicitar el contenido de la página y parsear el contenido HTML
url = 'https://www.promiedos.com.ar/'

response = requests.get(url)

soup = BeautifulSoup(response.content, 'html.parser')

info = soup.find_all('div', class_=re.compile(r'^\d+$'))

fecha = datetime.now().strftime("%Y-%m-%d")

data = [] # Inicializar una lista vacía para almacenar los datos de los partidos
#%% Extraer información de cada liga y partido
for liga in info:
    categoria = liga.find('a').text

    # Extraer los nombres de los equipos y agruparlos de dos en dos
    nombre_equipos = liga.find_all('span', class_='datoequipo')
    nombre_equipos = [nombre_equipos[i:i + 2] for i in range(0, len(nombre_equipos), 2)]

    # Extraer los resultados de los equipos
    resultados_equipos = list(zip(liga.find_all('td', class_='game-r1'), 
                                  liga.find_all('td', class_='game-r2')))[1:]

    # Extraer los horarios de los partidos
    hora = liga.find_all('td', class_=['game-fin', 'game-time', 'game-play'])[1:]

    # Extraer los datos adicionales sobre los goles
    goles = liga.find_all('tr', class_='goles')

    # Recorrer todos los equipos y resultados para crear las filas
    for i in range(len(nombre_equipos)):
        row = {
            'dia': fecha, 
            'horario': hora[i].text.strip(),  
            'equipo 1': nombre_equipos[i][0].text.strip(),  
            'goles1': resultados_equipos[i][0].text.strip(), 
            'goles2': resultados_equipos[i][1].text.strip(),  
            'equipo 2': nombre_equipos[i][1].text.strip(),  
            'categoria': categoria.strip()  
        }
        
        data.append(row) # Agregar los datos a la lista 'data'
#%% Insertar los datos en MongoDB si la lista 'data' no está vacía
if data:
    collection.insert_many(data) 
#%%

