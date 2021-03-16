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
filepattern01 = 'demograficos_capta'
fileext = ".dat"
staging_table1 = 'tmp_nombres'
table = 'Datos_generales.nombres'
pasos_proceso = 3
proceso = 'Carga nombre'
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
            load_sql1 += " (numcte, num_cte_coppel, sucursal, fecha_alta, apell_paterno, apell_materno, nombre1, nombre2,"
            load_sql1 += " fecha_nac, estado_civil, sexo, estado, ciudad, municipio, rfc);"
            cursor.execute(load_sql1)
            cf.logging_carga(cursor, filename, staging_table1)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo nombres')
    #Staging
            paso = 3
            staging_step_3a = "insert into " + table + " select * from Staging." + staging_table1 + " b"
            staging_step_3a += " on duplicate key update  num_cte_coppel = b.num_cte_coppel,"
            staging_step_3a += " apell_paterno = b.apell_paterno,"
            staging_step_3a += " apell_materno = b.apell_materno,"
            staging_step_3a += " nombre1 = b.nombre1,"
            staging_step_3a += " nombre2 = b.nombre2,"
            staging_step_3a += " fecha_nac = b.fecha_nac,"
            staging_step_3a += " estado_civil = b.estado_civil,"
            staging_step_3a += " sexo = b.sexo,"
            staging_step_3a += " sucursal = b.sucursal,"
            staging_step_3a += " fecha_alta = b.fecha_alta,"
            staging_step_3a += " estado = b.estado,"
            staging_step_3a += " municipio = b.municipio,"
            staging_step_3a += " ciudad = b.ciudad,"
            staging_step_3a += " rfc = b.rfc;"
            cursor.execute(staging_step_3a)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza tabla nombres')
        
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
