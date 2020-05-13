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
filepattern = 'Rep_transac_1eruso_tdc_'
fileext = ".txt"
staging_table = 'tmp_1er_transact'
table = 'Cuentas_tc.prim_actividad'
table_historico = 'Historicos.Primer_transact'
pasos_proceso = 6
proceso = 'Carga primera transaccion'
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

filename = filepattern + datetime.datetime.today().strftime("%y%m") + '10' + fileext
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
            load_sql += " fields terminated by ';' escaped by '' "
            load_sql += " lines terminated by '\n'"
            load_sql += " ignore 1 lines;"
            #print(filename)
            cursor.execute('truncate table Staging.' + staging_table + ';')
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa archivo rep_transac_1er')
    #Staging
            paso = 2
            staging_step_2a = "update Staging." + staging_table + " Set fecha_activacion = concat(substr(fecha_activacion,7,4), '-', substr(fecha_activacion,1,2), '-', substr(fecha_activacion,4,2))"
            staging_step_2a += "where length(fecha_activacion) = 10;"
            cursor.execute(staging_step_2a)
            staging_step_2b = "update Staging." + staging_table + " Set fecha_1a_transact = concat(substr(fecha_1a_transact,7,4), '-', substr(fecha_1a_transact,1,2), '-', substr(fecha_1a_transact,4,2))"
            staging_step_2b += "where length(fecha_1a_transact) = 10;"
            cursor.execute(staging_step_2b)
            staging_step_2c = "update Staging." + staging_table + " Set control1 = 'ok'"
            staging_step_2c += "where (length(fecha_activacion) = 10"
            staging_step_2c += " or length(fecha_1a_transact) = 10"
            staging_step_2c += ") and control1 is null;"
            cursor.execute(staging_step_2c)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_3b = "insert into " + table_historico 
            staging_step_3b += " select"
            staging_step_3b += " Num_credito,"
            staging_step_3b += " Fecha_activacion,"
            staging_step_3b += " Fecha_1a_transact,"
            staging_step_3b += " RFC,"
            staging_step_3b += " Negocio,"
            staging_step_3b += " Monto,"
            staging_step_3b += " Txn_compras_acum_mes,"
            staging_step_3b += " Vol_compras_acum_mes,"
            staging_step_3b += " Txn_disp_acum_mes,"
            staging_step_3b += " Vol_disp_acum_mes,"
            staging_step_3b += " '" + fecha_seguimiento + "'"
            staging_step_3b += " from Staging.tmp_1er_transact"
            staging_step_3b += " where Control1  = 'ok';"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Agrega a base histórica completa')
    
            paso = 4
            staging_step_4 = "insert into " + table + " select a.Num_credito, Fecha_1a_transact," 
            staging_step_4 += " case when Negocio like 'Dispo%' then 'Disposicion' else 'Compra' end as Transact"
            staging_step_4 += " from Staging." + staging_table + " a left join " + table + " b"
            staging_step_4 += " on a.num_credito = b.num_credito"
            staging_step_4 += " where b.num_credito is null;" 
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Integra a base historica nueva version')
    
            paso = 5
            staging_step_5b = "update Cuentas_tc.Cartera a, Cuentas_tc.prim_actividad b"
            staging_step_5b += " set a.f_primer_trxn = b.f_primer_trxn, a.primer_trxn = b.primer_trxn"
            staging_step_5b += " where a.num_credito = b.num_credito;"
            cursor.execute(staging_step_5b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza fechas en base de cartera del mes')
    
            paso = 6
            staging_step_6 = "update Cuentas_tc.CarteraConcentrado a, Cuentas_tc.prim_actividad b"
            staging_step_6 += " set a.f_primer_trxn = b.f_primer_trxn, a.primer_trxn = b.primer_trxn"
            staging_step_6 += " where a.num_credito = b.num_credito"
            staging_step_6 += " and a.f_primer_trxn is null;"
            cursor.execute(staging_step_6)
            cf.logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'Actualiza fechas en base de cartera concentrada')
           
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
