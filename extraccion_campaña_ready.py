# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 13:53:35 2020

@author: fespinosa
"""

import os
import pymysql
import warnings
import pandas as pd
import Export_functions as Export

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#funciones

    
#Constantes
final_table = 'Campañas.Regreso_clases_2021'
tipo = 'sms'
producto = 'Nunca'
path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\Proyectos\\2021\\202107\\campaña regreso a clases\\Envios\\sms\\'
pasos_proceso = 4
proceso = 'Extrae archivos ready'
Plantilla = 'PD'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

try:
    paso = 0
    con = pymysql.connect(host = host, 
                      user = user, 
                      password = password, 
                      port = port,
                      autocommit=True,
                      local_infile=1)
    cursor = con.cursor()

    paso = 1
    SQL_text = "Select num_cliente, email, nombres"
    SQL_text += ' from ' + final_table
    SQL_text += " where canal = '" + tipo + "'"
    SQL_text += " and Categoria = '" + producto + "'"
    #SQL_text += ' having count(*) > 5'
    SQL_text += ' ;'
    cursor.execute(SQL_text)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    paso = 3
#    n = 0    
    Export.exporta_archivos_ready(tipo,Plantilla,path,df)

except Exception as e:
    print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

    
