#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:34:44 2019
subject: Proceso de carga historica, donde se conoce las fechas disponibles
 y los archivos a cargar se encuentran en una carpeta especifica.
@author: francisco
"""

import os
import pymysql
import warnings
import datetime
import sys
import Complement_functions as cf

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('error', UserWarning)

#funciones


#Constantes
filepattern = 'trancheq'
fileext = ".txt"
#host = '192.168.0.28'
port = 3306
#user = 'root'
#password = 'Alb3rt-31nstein'
host= '10.26.211.46'
#user= 'analitics'
#password= '2017YdwVCs51may2'
user= 'c97635723'
password= '9AJG7ae4gAE3av4a'
staging_table = 'tmptrancheq'
table = 'Transaccional.Trancheq_2020'
pasos_proceso = 3
proceso = 'Carga trancheq'


for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename = filepattern + (datetime.datetime.today() - datetime.timedelta(1)).strftime("%m%d%Y") + fileext
if filename in files:
    load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
    load_sql += " fields terminated by '|' escaped by '' "
    load_sql += " lines terminated by '\n';"
    #print(filename)
   
    try:
        paso = 1
        con = pymysql.connect(host = host, 
                          user = user, 
                          password = password, 
                          port = port,
                          autocommit=True,
                          local_infile=1)
        cursor = con.cursor()
        if cf.Validacion_archivo(cursor, filename):
            print('Archivo previamente cargado: ' + filename)
            con.close()
            sys.exit()

        cursor.execute('truncate table Staging.' + staging_table + ';')
        cursor.execute(load_sql)
        cf.logging_carga(cursor, filename, staging_table)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo trancheq')
#Staging
        paso = 2
        staging_step_1a = "update Staging." + staging_table + " Set fechaoper = concat(substr(fechaoper,7,4), '-', substr(fechaoper,1,2), '-', substr(fechaoper,4,2)),"
        staging_step_1a += " field8 = 'ok'"
        staging_step_1a += " where field8 = '';"
        cursor.execute(staging_step_1a)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')

        paso = 3
        staging_step_2a = "insert into " + table
        staging_step_2a += " select * from Staging." + staging_table
        staging_step_2a += " where field8 = 'ok';"
        cursor.execute(staging_step_2a)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta en Trancheq')

        print('Proceso de carga terminado: ' + filename)

        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
