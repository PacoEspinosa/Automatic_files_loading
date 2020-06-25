#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:34:44 2019
subject: Proceso de carga historica, donde se conoce las fechas disponibles
 y los archivos a cargar se encuentran en una carpeta especifica.
@author: francisco
"""

import os
import pandas as pd
import pymysql
import warnings
import Complement_functions as cf

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('ignore')

#funciones

#Constantes
date1 = '2020-01-01'  
date2 = '2020-06-30'
transact_table = 'Transacciones_TDC_2020'
operative_table = 'Operativas'
filepattern = 'his_credito'
fileext = ""
table = 'tmphiscred'
pasos_proceso = 10
proceso = 'Carga transacciones'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

datelist = pd.date_range(date1,date2).tolist()

for date in datelist:
    filename = filepattern + date.strftime("%m%d%Y") + fileext
    if filename in files:
        load_sql = "load data local infile '" + filename + "' into table Staging." + table
        load_sql += " fields terminated by '|' escaped by '' "
        load_sql += " lines terminated by '\n';"
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
            if cf.Validacion_archivo(cursor,filename):
                print('Archivo previamente cargado: ' + filename)
                con.close()
                continue

            cursor.execute("truncate table Staging." + table + ';')
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo transacciones')
#Staging
            paso = 2
            staging_step_1 = "update Staging.tmphiscred Set fechaoper = concat(substr(fechaoper,7,4), '-', substr(fechaoper,1,2), '-', substr(fechaoper,4,2)),"
            staging_step_1 += "control = 'ok' where (control = '' or control is null);"
            cursor.execute(staging_step_1)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')

            paso = 3
            staging_step_2 = "insert into Historicos.Intereses select fechaoper, num_credito, Monto, descripcion, transaccion"
            staging_step_2 += " from Staging." + table
            staging_step_2 += " where transaccion in ('6940','4201','6608')"
            staging_step_2 += " and control = 'ok'"
            staging_step_2 += " and secuencia not in (2,3);"
            cursor.execute(staging_step_2)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta en el historico de intereses')

            paso = 4
            staging_step_3 = "insert into Transacciones." + transact_table + " (producto, num_credito, num_cliente, sucursal,"
            staging_step_3 += " folio_suc, num_tdc, monto, transaccion, contable, fechaoper)"
            staging_step_3 += " select producto, num_credito, num_cliente, sucursal,"
            staging_step_3 += " folio_suc, num_tdc, monto, transaccion, contable, fechaoper"
            staging_step_3 += " from Staging." + table
            staging_step_3 += " where transaccion in ('4200','4201','4202','4314','4401','5080','5212','5260','6212','6218',"
            staging_step_3 += "'6219','6220','6260','6282','6283','6510','6608','6609','6640','6800',"
            staging_step_3 += "'6830','6845','6846','6871','6873','6877','6890','6892','6893','6895',"
            staging_step_3 += "'6900','6901','6940','6952','6999','7021','7041','7139','7380','7381',"
            staging_step_3 += "'7382','7384','7386','7450','7577','7727','7728','7729','7746','7747',"
            staging_step_3 += "'7778','7779','7780','7781','7782','7896','7951','8022','8023','8024',"
            staging_step_3 += "'8025','8045','8046','8051','8071','8072','8102','8104','8105','8112',"
            staging_step_3 += "'8151','8195','8197','8231','8232','8233','8244','8245','8246','8275',"
            staging_step_3 += "'8309','8371')"
            staging_step_3 += " and control = 'ok'"
            staging_step_3 += " and secuencia not in (2,3);"
            cursor.execute(staging_step_3)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta transacciones financieras del dia')

            paso = 5
            staging_step_4 = "insert into Transacciones." + operative_table + " (producto, num_credito, num_cliente, sucursal, folio_suc, "
            staging_step_4 += " num_tdc, monto, descripcion, transaccion, fechaoper)"
            staging_step_4 += " select producto, num_credito, num_cliente, sucursal, folio_suc, "
            staging_step_4 += " num_tdc, monto, descripcion, transaccion, fechaoper"
            staging_step_4 += " from Staging." + table
            staging_step_4 += " where transaccion in ('4402','4403','6450','6451','6452','6453','6512','6515','6591','6600',"
            staging_step_4 += "'6655','6695','6696','6697','6702','7042','7044','7052','7141','7142',"
            staging_step_4 += "'7144','7152','7387','7393','7394','7402','7405','7408','7796','7800',"
            staging_step_4 += "'7806','7807','7808','7809','7901','7902','7910','7912','7952','7954',"
            staging_step_4 += "'7958','7960','7962','8026','8027','8033','8121','8255','8280','8281')"
            staging_step_4 += " and control = 'ok'"
            staging_step_4 += " and secuencia not in (2,3);"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta transacciones operativas')

            paso = 6
            staging_step_5 = "drop table if exists Staging.tmp_relacion_tarjeta;"
            cursor.execute(staging_step_5)
            cf.logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'Limpia tabla temporal de relaciones cuenta y tarjeta')
           
            paso = 7
            staging_step_6 = "create table Staging.tmp_relacion_tarjeta"
            staging_step_6 += " select producto, num_credito, num_cliente, num_tdc, max(fechaoper) as last_known"
            staging_step_6 += " from Staging." + table
            staging_step_6 += " where length(trim(num_tdc)) > 10"
            staging_step_6 += " and control = 'ok'"
            staging_step_6 += " group by producto, num_credito, num_cliente, num_tdc;"
            cursor.execute(staging_step_6)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Respalda relacion cuenta y tarjeta')

            paso = 8
            cursor.execute('truncate table Staging.tmp_dups_tdc')
            staging_step_7 = "insert into Staging.tmp_dups_tdc select num_tdc "
            staging_step_7 += " from Staging.tmp_relacion_tarjeta"
            staging_step_7 += " group by num_tdc "
            staging_step_7 += " having count(*) > 1;"
            cursor.execute(staging_step_7)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Identifica casos duplicados para analisis')

            paso = 9
            staging_step_8 = "insert into Cuentas_tc.Relacion_dups_tarjeta "
            staging_step_8 += " select producto, num_credito, num_cliente, a.num_tdc, max(fechaoper) as last_known "
            staging_step_8 += " from Staging.tmphiscred a, Staging.tmp_dups_tdc b "
            staging_step_8 += " where a.num_tdc = b.num_tdc"
            staging_step_8 += " group by producto, num_credito, num_cliente, a.num_tdc;"
            cursor.execute(staging_step_8)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Respalda casos duplicados para analisis')

            paso = 10
            staging_step_9 = "insert into Cuentas_tc.Relacion_tarjeta select * "
            staging_step_9 += " from Staging.tmp_relacion_tarjeta b"
            staging_step_9 += " on duplicate key update  last_known = b.last_known;"
            cursor.execute(staging_step_9)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Integra relacion diaria al historico')

            print('Proceso de carga terminado: ' + filename)

            con.close()
            
        except Exception as e:
            print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
    else:
        print('No se localizó el archivo: ' + filename)
    