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

warnings.simplefilter('error', UserWarning)

#funciones
#Constantes
filepattern01 = 'movs_aut_'
filepattern02 = 'movs_noaut_'
fileext = ".txt"
fileext01 = "a.txt"
fileext02 = "b.txt"
fileext03 = "c.txt"
staging_table = 'Movs_aut'
table = 'Transaccional.Transacciones_2020'
pasos_proceso = 5
proceso = 'Carga autorizaciones'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']


for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename1 = filepattern01 + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y%m") + fileext01
filename2 = filepattern01 + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y%m") + fileext02
filename3 = filepattern01 + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y%m") + fileext03
filename4 = filepattern02 + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y%m") + fileext
filename = filepattern01 + " / " + filepattern02 + " / " + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y%m")

if filename1 in files and filename2 in files and filename3 in files and filename4 in files:
    #print(filename1, " / ", filename2)
    try:
        con = pymysql.connect(host = host, 
                          user = user, 
                          password = password, 
                          port = port,
                          autocommit=True,
                          local_infile=1)
        cursor = con.cursor()
        if cf.Validacion_archivo(cursor, filename1) or cf.Validacion_archivo(cursor, filename2) or cf.Validacion_archivo(cursor, filename3) or cf.Validacion_archivo(cursor, filename4):
            print('Archivo previamente cargado: ' + filename)
            con.close()
        else:
            paso = 1
            cursor.execute('truncate table Staging.' + staging_table + ';')
            cf.logging_proceso(cursor,proceso + ': ' + filename ,pasos_proceso,paso,'Prepara tabla para carga')
    
            paso = 2
            load_sql1 = "load data local infile '" + filename1 + "' into table Staging." + staging_table
            load_sql1 += " fields terminated by '|' escaped by '' "
            load_sql1 += " lines terminated by '\n'"
            load_sql1 += " ignore 1 lines;"
            cursor.execute(load_sql1)
            cf.logging_carga(cursor, filename1, staging_table)
    
            load_sql2 = "load data local infile '" + filename2 + "' into table Staging." + staging_table
            load_sql2 += " fields terminated by '|' escaped by '' "
            load_sql2 += " lines terminated by '\n'"
            load_sql2 += " ignore 1 lines;"
            cursor.execute(load_sql2)
            cf.logging_carga(cursor, filename2, staging_table)

            load_sql3 = "load data local infile '" + filename3 + "' into table Staging." + staging_table
            load_sql3 += " fields terminated by '|' escaped by '' "
            load_sql3 += " lines terminated by '\n'"
            load_sql3 += " ignore 1 lines;"
            cursor.execute(load_sql3)
            cf.logging_carga(cursor, filename3, staging_table)

            load_sql4 = "load data local infile '" + filename4 + "' into table Staging." + staging_table
            load_sql4 += " fields terminated by '|' escaped by '' "
            load_sql4 += " lines terminated by '\n'"
            load_sql4 += " ignore 1 lines;"
            cursor.execute(load_sql4)
            cf.logging_carga(cursor, filename4, staging_table)

            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo autorizaciones')
    #Staging
            paso = 3
            staging_step_3a = "drop table if exists Staging.tmp_negocios_nvos;"
            cursor.execute(staging_step_3a)
            
            staging_step_3b = "create temporary table if not exists Staging.tmp_negocios_nvos ENGINE=MyISAM as"
            staging_step_3b += " select distinct infreceptor from Staging.Movs_aut;"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Crea catalogo de negocios del mes')
    
            paso = 4
            staging_step_4 = "insert into Catalogos.cat_clasificacion_negocios (infreceptor) "
            staging_step_4 += " select a.* from Staging.tmp_negocios_nvos a left join Catalogos.cat_clasificacion_negocios b "
            staging_step_4 += " on a.infreceptor = b.infreceptor"
            staging_step_4 += " where b.infreceptor is null;"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta negocios del mes en Catalogo general')
    
            paso = 5
            staging_step_5 = "insert into " + table + " select Secuencia,"
            staging_step_5 += " cod_iso,"
            staging_step_5 += " codgironeg,"
            staging_step_5 += " no_tarjeta,"
            staging_step_5 += " prodind,"
            staging_step_5 += " formato,"
            staging_step_5 += " codtran,"
            staging_step_5 += " fechaexptarj,"
            staging_step_5 += " codreversa,"
            staging_step_5 += " monto,"
            staging_step_5 += " infreceptor,"
            staging_step_5 += " idreceptor,"
            staging_step_5 += " idterminal,"
            staging_step_5 += " secuenciaorig,"
            staging_step_5 += " movreversado,"
            staging_step_5 += " esnacional,"
            staging_step_5 += " metodocaptura,"
            staging_step_5 += " motivo,"
            staging_step_5 += " trancajeropropio,"
            staging_step_5 += " substr(fechaoper, 1,10)"
            staging_step_5 += " FROM Staging." + staging_table + ";"
            cursor.execute(staging_step_5)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta informacion de autorizaciones')
               
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
