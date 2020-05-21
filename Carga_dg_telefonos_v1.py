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
filepattern = 'telefonos'
fileext = ".dat"
staging_table = 'tmp_telefonos'
staging_table1 = 'tmp_telefonos_01'
table = 'Datos_generales.Telefonos'
pasos_proceso = 9
proceso = 'Carga telefonos'
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

filename1 = filepattern + fileext
filename = filename1 + '-' + fecha_seguimiento

if filename1 in files:
 
    try: 
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
            paso = 1    
            cursor.execute('truncate table Staging.' + staging_table + ';')
            load_sql = "load data local infile '" + filename1 + "' into table Staging." + staging_table
            load_sql += " fields terminated by '\t' escaped by '' "
            load_sql += " lines terminated by '\n'"
            load_sql += " ignore 1 lines;"
            #print(filename)
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa_archivos_telefonos')
    #Staging
            paso = 2
            cursor.execute("truncate table Staging." + staging_table1 + ";")
            staging_step_2a = "insert into Staging." + staging_table1 + " (num_cliente)"
            staging_step_2a += " select distinct num_cliente"
            staging_step_2a += " from Staging." + staging_table + ";"
            cursor.execute(staging_step_2a)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Desduplica_registros')
    
            paso = 3
            staging_step_3b = "UPDATE Staging." + staging_table + " a, Staging." + staging_table1 + " b SET b.celular = a.celular "
            staging_step_3b += " WHERE b.num_cliente = a.num_cliente"
            staging_step_3b += " and a.celular <> ''"
            staging_step_3b += " and b.celular is null;"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza_celular')
    
            paso = 4
            staging_step_4 = "UPDATE Staging." + staging_table + " a, Staging." + staging_table1 + " b SET b.casa = a.casa "
            staging_step_4 += " WHERE b.num_cliente = a.num_cliente"
            staging_step_4 += " and a.casa <> ''"
            staging_step_4 += " and b.casa is null;"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza_tel_casa')
    
            paso = 5
            staging_step_5 = "UPDATE Staging." + staging_table + " a, Staging." + staging_table1 + " b SET b.oficina = a.oficina "
            staging_step_5 += " WHERE b.num_cliente = a.num_cliente"
            staging_step_5 += " and a.oficina <> ''"
            staging_step_5 += " and b.oficina is null;"
            cursor.execute(staging_step_5)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza_tel_oficina')
    
            paso = 6
            staging_step_6 = "UPDATE Staging." + staging_table + " a, Staging." + staging_table1 + " b SET b.otro = a.otro "
            staging_step_6 += " WHERE b.num_cliente = a.num_cliente"
            staging_step_6 += " and a.otro <> ''"
            staging_step_6 += " and b.otro is null;"
            cursor.execute(staging_step_6)
            cf.logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'Actualiza_tel_otros')
           
            paso = 7
            staging_step_7 = "UPDATE Staging." + staging_table + " a, Staging." + staging_table1 + " b SET b.extension = a.extension "
            staging_step_7 += " WHERE b.num_cliente = a.num_cliente"
            staging_step_7 += " and a.extension <> ''"
            staging_step_7 += " and b.extension is null;"
            cursor.execute(staging_step_7)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Actualiza_extension')
    
            paso = 8
            cursor.execute("update Staging.tmp_telefonos_01 set casa = null where casa = '0000000000';")
            cursor.execute("update Staging.tmp_telefonos_01 set celular = null where celular = '0000000000';")
            cursor.execute("update Staging.tmp_telefonos_01 set celular = null where celular = '00000000000';")
            cursor.execute("update Staging.tmp_telefonos_01 set celular = null where celular = '000000000000';")
            cursor.execute("update Staging.tmp_telefonos_01 set oficina = null where oficina = '0000000000';")
            cursor.execute("update Staging.tmp_telefonos_01 set otro = null where otro = '0000000000';")
            cursor.execute("update Staging.tmp_telefonos_01 set celular = substr(celular,2,10) where length(celular) = 11;")
            cursor.execute("update Staging.tmp_telefonos_01 set celular = substr(celular,3,10) where length(celular) = 12;")
            cursor.execute("update Staging.tmp_telefonos_01 set celular = substr(celular,4,10) where length(celular) = 13;")
            cursor.execute("update Staging.tmp_telefonos_01 set casa = substr(casa,2) where casa like '0%';")
            cursor.execute("update Staging.tmp_telefonos_01 set oficina = substr(oficina,2) where oficina like '0%';")
            cursor.execute("update Staging.tmp_telefonos_01 set otro = substr(otro,2) where otro like '0%';")
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Validaciones_varias_numeros')

            paso = 9
            staging_step_9 = "insert into " + table + " (num_cliente, casa, celular, oficina, otro, extension)"
            staging_step_9 += " select b.num_cliente, b.casa, b.celular, b.oficina, b.otro, b.extension"
            staging_step_9 += " from Staging.tmp_telefonos_01 b"
            staging_step_9 += " on duplicate key update casa = b.casa, celular = b.celular, oficina = b.oficina, otro = b.otro, extension = b.extension;"
            cursor.execute(staging_step_9)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Actualiza_numeros_en_base_general')

   
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
