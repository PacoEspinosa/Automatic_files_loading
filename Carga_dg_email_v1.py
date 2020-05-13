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
filepattern01 = 'Cuentas_con_email'
fileext = ".dat"
staging_table1 = 'tmp_email'
table = 'Datos_generales.email'
pasos_proceso = 3
proceso = 'Carga correos'
fecha_seguimiento = datetime.datetime.today().strftime("%Y-%m")

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename1 = filepattern01 + fileext
filename = filename1 + '-' + fecha_seguimiento

if filename1 in files:
    #print(filename1, " / ", filename2)
    try:
        con = pymysql.connect(host = host, 
                          user = user, 
                          password = password, 
                          port = port,
                          autocommit=True,
                          local_infile=1)
        cursor = con.cursor()
        if cf.Validacion_archivo(cursor, (filename)):
            print('Archivo previamente cargado: ' + filename)
            con.close()
        else:
            paso = 1
            cursor.execute('truncate table Staging.' + staging_table1 + ';')
            cf.logging_proceso(cursor,proceso + ': ' + filename ,pasos_proceso,paso,'Prepara tabla para carga')
    
            paso = 2
            load_sql1 = "load data local infile '" + filename1 + "' into table Staging." + staging_table1
            load_sql1 += " fields terminated by '\t' escaped by '' "
            load_sql1 += " lines terminated by '\n'"
            load_sql1 += " ignore 1 lines"
            load_sql1 += " (numcte, num_credito, correo_elec, valido, status_correo);"
            cursor.execute(load_sql1)
            cf.logging_carga(cursor, filename, staging_table1)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo correos')
    #Staging
            paso = 3
            staging_step_3a = "truncate table " + table + ";"
            cursor.execute(staging_step_3a)
            staging_step_3b = "insert into " + table + " select * from Staging." + staging_table1 + ";"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza tabla mensual')
        
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
