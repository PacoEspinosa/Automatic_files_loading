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
import Complement_functions as cf

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#funciones
    

#Constantes
filepattern = 'OutBehavioral_'
fileext = ".txt"
staging_table_segmentado = 'tmpbehavioralv1'
staging_table = 'tmpbehavioral'
table = 'Cuentas_tc.OutBehavioral'
pasos_proceso = 3
proceso = 'Carga calificacion de comportamiento'
fecha_seguimiento = datetime.datetime.today().strftime("%Y-%m-%d")
dias_mes = '30'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)
        
"""
if (datetime.datetime.today() - datetime.timedelta(50)).strftime("%m") in ('01','03','05','',''):
    dias_mes = '31'
elif (datetime.datetime.today() - datetime.timedelta(50)).strftime("%m") = '02':
    dias_mes = '28'
else:
    dias_mes = '30'
"""

#filename = filepattern + dias_mes + (datetime.datetime.today() - datetime.timedelta(50)).strftime("%m%Y") + fileext
for filename in files:

    try:
        paso = 0
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
        else:
            filetype = input('Tipo de archivo a cargar Segmentado/Oneline: ')
            if filetype == 'Segmentado':
                paso = 1
                load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table_segmentado
                load_sql += " fields terminated by ';' optionally enclosed by '\"'"
                load_sql += " lines terminated by '\r\n'"
                load_sql += " ignore 1 lines;"
                #print(filename)
                cursor.execute('truncate table Staging.' + staging_table_segmentado + ';')
                cursor.execute(load_sql)
                cf.logging_carga(cursor, filename, staging_table_segmentado)
                cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa archivo OutBehavioral')
        #Staging
                paso = 2
                staging_step_2a = " truncate table " + table + ";"
                cursor.execute(staging_step_2a)
                staging_step_2b = "insert ignore into " + table + " (num_credito, Designación, Calificación)"
                staging_step_2b += " select distinct num_credito, segmento, calificacion"
                staging_step_2b += " from Staging." + staging_table_segmentado + ";"
                cursor.execute(staging_step_2b)
                cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta registros actuales')
                print('Proceso de carga terminado: ' + filename)
            elif filetype == 'Oneline':
                paso = 1
                load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
                load_sql += " fields terminated by '|' escaped by '' "
                load_sql += " lines terminated by '\n'"
                load_sql += " ;"
                #print(filename)
                cursor.execute('truncate table Staging.' + staging_table + ';')
                cursor.execute(load_sql)
                cf.logging_carga(cursor, filename, staging_table)
                cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa archivo OutBehavioral')
        #Staging
                paso = 2
                staging_step_2a = " truncate table " + table + ";"
                cursor.execute(staging_step_2a)
                staging_step_2b = "insert ignore into " + table 
                staging_step_2b += " select distinct substr(registro,4,12) as num_credito, trim(substr(registro,31,12)) as designacion,"
                staging_step_2b += " abs(substr(registro,43,5)) as calificacion, trim(substr(registro,55,9)) as riesgo"
                staging_step_2b += " from Staging." + staging_table + ";"
                cursor.execute(staging_step_2b)
                cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta registros actuales')
                print('Proceso de carga terminado: ' + filename)
            else:
                print('Proporciona opcion válida')
                filepattern = '__'
    
           
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

if files == []:
    print('No se localizaron archivos de carga')
