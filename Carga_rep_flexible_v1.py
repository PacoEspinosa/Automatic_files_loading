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
filepattern = 'Reporte_P_Flexible_'
fileext = ".txt"
staging_table = 'tmp_rep_flexible'
table = 'Cuentas_tc.Rep_flexible'
historic_table = 'Historicos.Rep_flexible'
pasos_proceso = 4
proceso = 'Carga reporte flexible'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)

#filename = filepattern + datetime.datetime.today().strftime("%d%m%Y") + fileext
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
            paso = 1
            load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
            load_sql += " fields terminated by '|' escaped by '' "
            load_sql += " lines terminated by '\n'"
            load_sql += " ignore 1 lines;"
            #print(filename)
            cursor.execute('truncate table Staging.' + staging_table + ';')
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo solicitudes')
    #Staging
            paso = 2
            staging_step_1a = "update Staging." + staging_table + " Set fecha_disp = concat(substr(fecha_disp,7,4), '-', substr(fecha_disp,4,2), '-', substr(fecha_disp,1,2))"
            staging_step_1a += ", control = 'ok'"
            staging_step_1a += "where length(fecha_disp) = 10;"
            cursor.execute(staging_step_1a)
            staging_step_1b = "update Staging." + staging_table + " Set fecha_ult_pago = concat(substr(fecha_ult_pago,7,4), '-', substr(fecha_ult_pago,4,2), '-', substr(fecha_ult_pago,1,2))"
            staging_step_1b += ", control = 'ok'"
            staging_step_1b += "where length(fecha_ult_pago) = 10;"
            cursor.execute(staging_step_1b)
            staging_step_1c = "update Staging." + staging_table + " Set fecha_apertura = concat(substr(fecha_apertura,7,4), '-', substr(fecha_apertura,4,2), '-', substr(fecha_apertura,1,2))"
            staging_step_1c += ", control = 'ok'"
            staging_step_1c += "where length(fecha_apertura) = 10;"
            cursor.execute(staging_step_1c)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_2a = "truncate Cuentas_tc.Rep_flexible;"
            cursor.execute(staging_step_2a)
            staging_step_2b = "insert into Cuentas_tc.Rep_flexible select num_credito,"
            staging_step_2b += " num_disposición,"
            staging_step_2b += " num_cliente,"
            staging_step_2b += " suc,"
            staging_step_2b += " nombre_suc,"
            staging_step_2b += " monto_disp,"
            staging_step_2b += " fecha_disp,"
            staging_step_2b += " plazo,"
            staging_step_2b += " fecha_ult_pago,"
            staging_step_2b += " estatus_credito,"
            staging_step_2b += " saldo_insoluto,"
            staging_step_2b += " capital_vigente,"
            staging_step_2b += " capital_transitorio,"
            staging_step_2b += " capital_vencido,"
            staging_step_2b += " meses_vencido,"
            staging_step_2b += " fecha_apertura,"
            staging_step_2b += " linea_credito"
            staging_step_2b += " from Staging.tmp_rep_flexible"
    #        staging_step_2b += " where control = 'ok';"
            cursor.execute(staging_step_2b)
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

if files == []:
    print('No se localizaron archivos de carga')
