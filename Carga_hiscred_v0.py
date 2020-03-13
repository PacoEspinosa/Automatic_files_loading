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
#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

#funciones
def logging_carga(cursor_con, filename, staging_table):
    mysql_log_load = "insert into Operacion_datamart.Importacion "
    mysql_log_load += " select curdate() as fecha, '" + filename + "' as nombre_archivo, user() as Usuario, count(*) as Registros from Staging." + staging_table + "; ";
    cursor_con.execute(mysql_log_load)
    
def logging_proceso(cursor_con, process, total_steps, step, descripcion):
    Etapa = str(step) + "/" + str(total_steps) + ") " + descripcion
    mysql_log_task = "insert into Operacion_datamart.Logs_procesos (fecha, Proceso, Etapa) "
    mysql_log_task += " select now() as fecha, '" + process + "' , '" + Etapa + "';"
    #print(mysql_log_task)
    cursor_con.execute(mysql_log_task)
    

#Constantes
date1 = '2020-03-06'  
date2 = '2020-03-09'
transact_table = 'Transacciones_TDC_2020'
operative_table = 'Operativas'
filepattern = 'his_credito'
fileext = ""
#host = '192.168.0.28'
port = 3306
#user = 'root'
#password = 'Alb3rt-31nstein'
host= '10.26.211.46'
#user= 'analitics'
#password= '2017YdwVCs51may2'
user= 'c97635723'
password= '9AJG7ae4gAE3av4a'
table = 'tmphiscred'
pasos_proceso = 10
proceso = 'Carga transacciones'


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
            cursor.execute("truncate table Staging." + table + ';')
            cursor.execute(load_sql)
#            logging_carga(cursor, filename, table)
#            logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo transacciones')
            paso = 2
            staging_step_1 = "update Staging.tmphiscred Set fechaoper = concat(substr(fechaoper,7,4), '-', substr(fechaoper,1,2), '-', substr(fechaoper,4,2)),"
            staging_step_1 += "control = 'ok' where (control = '' or control is null);"
            cursor.execute(staging_step_1)

            paso = 5
            staging_step_4 = "insert into Transacciones." + operative_table + " (producto, num_credito, num_cliente, sucursal, folio_suc, "
            staging_step_4 += " num_tdc, monto, descripcion, transaccion, fechaoper)"
            staging_step_4 += " select producto, num_credito, num_cliente, sucursal, folio_suc, "
            staging_step_4 += " num_tdc, monto, descripcion, transaccion, fechaoper"
            staging_step_4 += " from Staging." + table
            staging_step_4 += " where transaccion in ('6450','6451','6452','6453','6512','6515','6591','6600','6655','6695',"
            staging_step_4 += "'6696','6697','6702','7021','7042','7044','7052','7141','7142','7144',"
            staging_step_4 += "'7152','7387','7393','7394','7402','7405','7408','7796','7800','7806',"
            staging_step_4 += "'7807','7808','7809','7901','7902','7910','7912','7952','7954','7958',"
            staging_step_4 += "'7960','7962','8026','8027','8033','8121','8255','8280','8281')"
            staging_step_4 += " and control = 'ok'"
            staging_step_4 += " and secuencia not in (2,3);"
            cursor.execute(staging_step_4)
            print('Proceso de carga terminado: ' + filename)

            con.close()
            
        except Exception as e:
            print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
    else:
        print('No se localizó el archivo: ' + filename)
    