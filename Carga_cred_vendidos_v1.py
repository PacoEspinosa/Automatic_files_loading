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
filepattern = 'Creditos_Vendidos'
fileext = ".txt"
staging_table = 'tmp_vendidos'
table = 'Cuentas_tc.CreditoVendidos'
pasos_proceso = 3
proceso = 'Carga creditos vendidos'
fecha_seguimiento = datetime.datetime.today().strftime("%Y-%m-%d")

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename = filepattern + '05' + datetime.datetime.today().strftime("%m%Y") + fileext
if filename in files:
   
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
            load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
            load_sql += " fields terminated by '|' escaped by '' "
            load_sql += " lines terminated by '\n'"
            load_sql += " ;"
            #print(filename)
            cursor.execute('truncate table Staging.' + staging_table + ';')
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa archivo rep_transac_1er')
    #Staging
            paso = 2
            staging_step_2a = "update Staging." + staging_table + " Set Fecha = concat(substr(Fecha,7,4), '-', substr(Fecha,1,2), '-', substr(Fecha,4,2))"
            staging_step_2a += ", control = 'ok'"
            staging_step_2a += "where length(Fecha) = 10;"
            cursor.execute(staging_step_2a)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_3b = "insert into " + table 
            staging_step_3b += " select"
            staging_step_3b += " a.fecha,"
            staging_step_3b += " a.num_credito,"
            staging_step_3b += " a.Num_cliente,"
            staging_step_3b += " a.Saldo,"
            staging_step_3b += " a.pago_min_pdo,"
            staging_step_3b += " a.cifra1,"
            staging_step_3b += " a.cifra7,"
            staging_step_3b += " a.cifra2,"
            staging_step_3b += " a.meses_vencidos"
            staging_step_3b += " from Staging." + staging_table + " a left join " + table + " b"
            staging_step_3b += " on a.num_credito = b.num_credito"
            staging_step_3b += " where b.num_credito is null"
            staging_step_3b += " and Control  = 'ok';"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Agrega a base histórica completa')
            
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
