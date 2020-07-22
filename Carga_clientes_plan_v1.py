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

warnings.simplefilter('ignore')

#funciones

    
#Constantes
file = 'tdc'
if file == 'tdc':
    filepattern = 'ctes_prog_apoyo2020_'
    fileext = ".csv"
    staging_table = 'tmp_clientes_plan_apoyo'
    table = 'Cuentas_tc.Clientes_plan_apoyo'
    historic_table = ''
    pasos_proceso = 4
    proceso = 'Carga clientes en plan tdc'
else:
    filepattern = 'ctes_prog_apoyo2020crd_'
    fileext = ".csv"
    staging_table = 'tmp_clientes_plan_apoyocrd'
    table = 'Cuentas_tc.Clientes_plan_apoyocrd'
    historic_table = ''
    pasos_proceso = 4
    proceso = 'Carga clientes en plan pp'
    
#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename = filepattern + datetime.datetime.today().strftime("%Y%m%d") + fileext
if filename in files:
   
    try:
        paso = 0
        if cf.Validacion_archivo(cursor, filename):
            print('Archivo previamente cargado: ' + filename)
            con.close()
        
        else:
            con = pymysql.connect(host = host, 
                              user = user, 
                              password = password, 
                              port = port,
                              autocommit=True,
                              local_infile=1)
            cursor = con.cursor()
            
            paso = 1
            load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
            load_sql += " fields terminated by ',' escaped by '' "
            load_sql += " lines terminated by '\n'"
            load_sql += ";"
            #print(filename)
            cursor.execute('truncate table Staging.' + staging_table + ';')
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo clientes plan')
    #Staging
            paso = 2
            staging_step_2a = "update Staging." + staging_table + " Set fecha_apoyo = concat(substr(fecha_apoyo,7,4), '-', substr(fecha_apoyo,4,2), '-', substr(fecha_apoyo,1,2))"
            staging_step_2a += ", control = 'ok'"
            staging_step_2a += "where length(fecha_apoyo) = 10;"
            cursor.execute(staging_step_2a)
            staging_step_2b = "update Staging." + staging_table + " Set fecha_ins = concat(substr(fecha_ins,7,4), '-', substr(fecha_ins,4,2), '-', substr(fecha_ins,1,2))"
            staging_step_2b += ", control = 'ok'"
            staging_step_2b += "where length(fecha_ins) = 10;"
            cursor.execute(staging_step_2b)
            staging_step_2c = "update Staging." + staging_table + " Set fecha_apertura = concat(substr(fecha_apertura,7,4), '-', substr(fecha_apertura,4,2), '-', substr(fecha_apertura,1,2))"
            staging_step_2c += ", control = 'ok'"
            staging_step_2c += "where length(fecha_apertura) = 10;"
            cursor.execute(staging_step_2c)
            staging_step_2d = "update Staging." + staging_table + " Set fecha_ultimo_pago = concat(substr(fecha_ultimo_pago,7,4), '-', substr(fecha_ultimo_pago,4,2), '-', substr(fecha_ultimo_pago,1,2))"
            staging_step_2d += ", control = 'ok'"
            staging_step_2d += "where length(fecha_ultimo_pago) = 10;"
            cursor.execute(staging_step_2d)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_3a = "truncate " + table + ";"
            cursor.execute(staging_step_3a)
            staging_step_3b = "insert into " + table + " select * "
            staging_step_3b += " from Staging." + staging_table
            staging_step_3b += ";"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros en tabla del mes')
    
            paso = 4
            staging_step_3 = "insert into Historicos.Rep_flexible select " + datetime.datetime.today().strftime("%Y-%m-%d") + ","
            staging_step_3 += " num_credito,"
            staging_step_3 += " num_disposición,"
            staging_step_3 += " num_cliente,"
            staging_step_3 += " suc,"
            staging_step_3 += " nombre_suc,"
            staging_step_3 += " monto_disp,"
            staging_step_3 += " fecha_disp,"
            staging_step_3 += " plazo,"
            staging_step_3 += " fecha_ult_pago," 
            staging_step_3 += " estatus_credito,"
            staging_step_3 += " saldo_insoluto,"
            staging_step_3 += " capital_vigente,"
            staging_step_3 += " capital_transitorio,"
            staging_step_3 += " capital_vencido,"
            staging_step_3 += " meses_vencido,"
            staging_step_3 += " fecha_apertura,"
            staging_step_3 += " linea_credito"
            staging_step_3 += " from Cuentas_tc.Rep_flexible;"
            cursor.execute(staging_step_3)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros en tabla historica')
    
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
