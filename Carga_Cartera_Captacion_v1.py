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
filepattern = 'archsdocap'
fileext = ""
staging_table = 'Staging.tmp_portafolio_captacion'
historic_table = 'Historicos.Portafolio_captacion'
general_table = 'Cuentas_tc.Portafolio_captacion_concentrado'
current_table = 'Cuentas_tc.Portafolio_captacion'
pasos_proceso = 6
proceso = 'Carga Cartera Captacion'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)

for filename in files:
    fecha_seguimiento = filename[10:14] + '-' +  filename[14:16] #+ '-' +  filename[16:28]
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
            paso = 1
            staging_step_1a = "Truncate table " + staging_table + ";"
            cursor.execute(staging_step_1a)
            load_sql1 = "load data local infile '" + filename + "'"
            load_sql1 += " into table " + staging_table
            load_sql1 += " fields terminated by '|' escaped by '' "
            load_sql1 += " lines terminated by '\n';"
            cursor.execute(load_sql1)
            cf.logging_proceso(cursor,proceso + ': ' + filename ,pasos_proceso,paso,'Carga archivo solicitudes')
    
    #Staging
            paso = 2
            staging_step_2a = "update " + staging_table 
            staging_step_2a += " Set fecha_apertura = concat(substr(fecha_apertura,7,4), '-', substr(fecha_apertura,1,2), '-', substr(fecha_apertura,4,2))"
            staging_step_2a += ", fecha_seguimiento = concat(substr(fecha_seguimiento,7,4), '-', substr(fecha_seguimiento,1,2), '-', substr(fecha_seguimiento,4,2))"
            staging_step_2a += ", control = 'ok'"
            staging_step_2a += " where control = '';"
            cursor.execute(staging_step_2a)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_3 = "insert ignore into " + historic_table
            staging_step_3 += " select num_cuenta, num_cliente, fecha_seguimiento, saldo_fin_mes, tasa, status "
            staging_step_3 += " from " + staging_table
            staging_step_3 += " where control = 'ok';"
            cursor.execute(staging_step_3)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta información financiera en tabla histórica')

            paso = 4
            staging_step_4 = "insert ignore into " + current_table 
            staging_step_4 += " select Suc_origen, num_cuenta, num_cliente, num_ejecutivo, fecha_apertura,"
            staging_step_4 += " fecha_seguimiento, saldo_fin_mes, tasa, status"
            staging_step_4 += " from " + staging_table
            staging_step_4 += " where control = 'ok';"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta información financiera en tabla mes actual')
    
            paso = 5
            staging_step_5 = "insert ignore into " + general_table 
            staging_step_5 += " (Suc_origen, num_cuenta, num_cliente, num_ejecutivo, fecha_apertura,"
            staging_step_5 += " tasa, status)"
            staging_step_5 += " select a.Suc_origen, a.num_cuenta, a.num_cliente, a.num_ejecutivo, a.fecha_apertura,"
            staging_step_5 += " a.tasa, a.status"
            staging_step_5 += " from " + current_table + " a left join " + general_table + " b"
            staging_step_5 += " on a.num_cuenta = b.num_cuenta"
            staging_step_5 += " where b.num_cuenta is null;"
            cursor.execute(staging_step_5)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta en portafolio concentrado')
    
            paso = 6
            staging_step_6 = " update " + general_table + " a, " + current_table + " b " 
            staging_step_6 += " set a.tasa = b.tasa, a.status = b.status"
            staging_step_6 += " where a.num_cuenta = b.num_cuenta"
            staging_step_6 += " and (a.tasa <> b.tasa or a.status <> b.status);"
            cursor.execute(staging_step_6)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza status portafolio concentrado')

            paso = 7
            staging_step_7 = " update " + general_table + " a left join " + current_table + " b " 
            staging_step_7 += " on a.num_cuenta = b.num_cuenta"
            staging_step_7 += " set a.status = 99, Fecha_cancelacion = '" + fecha_seguimiento + "'"
            staging_step_7 += " where b.num_cuenta is null;"
            cursor.execute(staging_step_7)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza status cuentas eliminadas')
           
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

print('No se localizó el archivo: ' + filename)
