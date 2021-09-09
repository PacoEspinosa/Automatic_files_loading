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
import fnmatch
import Complement_functions as cf
#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#funciones


#Constantes
filepattern = 'Autorizadas_'  #Autorizadas_/Prestamos_
fileext = ".txt"
familia = 'TDC' if filepattern == 'Autorizadas_' else 'PP'
staging_table = 'tmp_aut_sin_recoger'
table = 'Campañas.Aut_sin_recoger'
pasos_proceso = 3
proceso = 'Carga ASR_' + familia
fecha_seguimiento = datetime.datetime.today().strftime("%Y%m%d")

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

for file in os.listdir('.'):
    if fnmatch.fnmatch(file, (filepattern + '*')):
        #print(file)
        files.append(file)

#filename = filepattern + datetime.datetime.today().strftime("%Y%m%d") + fileext
for filename in files:
    load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
    load_sql += " fields terminated by '|' escaped by '' "
    load_sql += " lines terminated by '\n'"
    load_sql += " ignore 1 lines;"
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
            continue
        
        cursor.execute('truncate table Staging.' + staging_table + ';')
        cursor.execute(load_sql)
        cf.logging_carga(cursor, filename, staging_table)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo ASR')
#Staging
        paso = 2
        staging_step_1a = "update Staging." + staging_table + " Set Fecha_autorizacion = concat(substr(Fecha_autorizacion,7,4), '-', substr(Fecha_autorizacion,1,2), '-', substr(Fecha_autorizacion,4,2))"
        staging_step_1a += ", Fecha_limite = concat(substr(Fecha_limite,7,4), '-', substr(Fecha_limite,1,2), '-', substr(Fecha_limite,4,2))"
        staging_step_1a += ", Archivo = '" + filename + "'"
        staging_step_1a += ", Familia = '" + familia + "'"
        staging_step_1a += ", Producto = case when substr(num_credito,1,2) = '60' then 'Clasica' when substr(num_credito,1,2) = '81' then 'Oro'"
        staging_step_1a += " when substr(num_credito,1,2) = '70' then 'Platinum' when substr(num_credito,1,2) = '66' then 'Básica'"
        staging_step_1a += " when substr(num_credito,1,2) = '78' then 'ADN' when substr(num_credito,1,2) = '61' then 'Reestructura' "
        staging_step_1a += " when substr(num_credito,1,2) = '63' then 'PP_12' when substr(num_credito,1,2) = '69' then 'PFB'"
        staging_step_1a += " when substr(num_credito,1,2) = '76' then 'PP_18' when substr(num_credito,1,2) = '77' then 'PP_24'"
        staging_step_1a += " when substr(num_credito,1,2) = '68' then 'Flexible' when substr(num_credito,1,2) = '85' then 'G. Coppel' else 'S/P' end"
        staging_step_1a += ", control = 'ok'"
        staging_step_1a += " where control is null;"
        cursor.execute(staging_step_1a)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')

        paso = 3
        staging_step_2a = "insert into Campañas.Aut_sin_recoger select Tipo_Promocion,"
        staging_step_2a += " Tipo_Logica,"
        staging_step_2a += " num_credito,"
        staging_step_2a += " num_cliente,"
        staging_step_2a += " Estado,"
        staging_step_2a += " Fecha_autorizacion,"
        staging_step_2a += " Fecha_limite,"
        staging_step_2a += " " + fecha_seguimiento + ","
        staging_step_2a += " Archivo,"
        staging_step_2a += " Familia,"
        staging_step_2a += " Producto"
        staging_step_2a += " from Staging.tmp_aut_sin_recoger"
        staging_step_2a += " where control = 'ok';"
        cursor.execute(staging_step_2a)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Incorpora registros dela semana')



        print('Proceso de carga terminado: ' + filename)

        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

