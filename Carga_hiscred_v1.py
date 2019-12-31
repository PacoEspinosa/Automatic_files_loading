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
    
def logging_proceso(cursor_con, process, total_steps, step, staging_table):
    mysql_log_task = "insert into Operacion_datamart.Importacion "
    mysql_log_task += " select curdate() as fecha, '" + filename + "' as nombre_archivo, user() as Usuario, count(*) as Registros from Staging." + staging_table + "; ";
    cursor_con.execute(mysql_log_task)
    

#Constantes
date1 = '2016-01-01' 
date2 = '2016-12-31'
transact_table = 'Transacciones_TDC_2016'
operative_table = 'Operativas'
filepattern = 'hiscred'
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

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

datelist = pd.date_range(date1,date2).tolist()

for date in datelist:
    filename = filepattern + date.strftime("%m%d%Y") + fileext
    if filename in files:
        load_sql = "load data local infile '" + filename + "' to table Staging." + table
        load_sql += " fields terminated by ',' ignore 1 lines;"
        #print(filename)
       
        try:
            con = pymysql.connect(host = host, 
                              user = user, 
                              password = password, 
                              port = port,
                              autocommit=True,
                              local_infile=1)
            cursor = con.cursor()
            cursor.execute("truncate table Staging." + table + ';')
            #cursor.execute(load_sql)
            logging_carga(cursor, filename, table)
#Staging
            staging_step_1 = "update Staging.tmphiscred Set fechaoper = concat(substr(fechaoper,7,4), "-", substr(fechaoper,1,2), "-", substr(fechaoper,4,2)),"
            staging_step_1 += "control = 'ok' where (control = '' or control is null);"
            cursor.execute(staging_step_1)

            staging_step_2 = "insert into Historicos.Intereses select fechaoper, num_credito, Monto, descripcion, transaccion"
            staging_step_2 += " from Staging." + table
            staging_step_2 += " where transaccion in ('6940','4201','6608')"
            staging_step_2 += " and control = 'ok';"
            cursor.execute(staging_step_2)

            staging_step_3 = "insert into Transacciones." + transact_table + " (producto, num_credito, num_cliente, sucursal,"
            staging_step_3 += " folio_suc, num_tdc, monto, descripcion, transaccion, contable, fechaoper)"
            staging_step_3 += " select producto, num_credito, num_cliente, sucursal,"
            staging_step_3 += " folio_suc, num_tdc, monto, descripcion, transaccion, contable, fechaoper"
            staging_step_3 += " from Staging." + table
            staging_step_3 += " where transaccion in ('4200','4201','4202','4314','4401','4402','4403','5080','5212','5260',"
            staging_step_3 += "'6212','6218','6219','6220','6260','6282','6283','6510','6608','6609',"
            staging_step_3 += "'6640','6800','6830','6845','6846','6871','6873','6877','6890','6892',"
            staging_step_3 += "'6893','6895','6900','6901','6940','6952','6999','7041','7139','7380',"
            staging_step_3 += "'7381','7382','7384','7386','7450','7577','7727','7728','7729','7746',"
            staging_step_3 += "'7747','7778','7779','7780','7781','7782','7896','7951','8022','8023',"
            staging_step_3 += "'8024','8025','8045','8046','8051','8071','8072','8102','8104','8105',"
            staging_step_3 += "'8112','8151','8195','8197','8231','8232','8233','8275')"
            staging_step_3 += " and control = 'ok';"
            cursor.execute(staging_step_3)

            staging_step_4 = "insert into Transacciones." + operative_table + " (producto, num_credito, num_cliente, sucursal,"
            staging_step_4 += " num_tdc, monto, descripcion, transaccion, fechaoper)"
            staging_step_4 += " select producto, num_credito, num_cliente, sucursal,"
            staging_step_4 += " num_tdc, monto, descripcion, transaccion, fechaoper"
            staging_step_4 += " from Staging." + table
            staging_step_4 += " where transaccion in ('6450','6451','6452','6453','6512','6515','6591','6600','6655','6695',"
            staging_step_4 += "'6696','6697','6702','7021','7042','7044','7052','7141','7142','7144',"
            staging_step_4 += "'7152','7387','7393','7394','7402','7405','7408','7796','7800','7806',"
            staging_step_4 += "''7807','7808','7809','7901','7902','7910','7912','7952','7954','7958',"
            staging_step_4 += "'7960','7962','8026','8027','8033','8121','8255','8280','8281')"
            staging_step_4 += " and control = 'ok';"
            cursor.execute(staging_step_4)

            staging_step_5 = "drop table Staging.tmp_relacion_tarjeta;"
            cursor.execute(staging_step_5)
            
            staging_step_6 = "create table Staging.tmp_relacion_tarjeta"
            staging_step_6 += " select producto, num_credito, num_cliente, num_tdc, max(fechaoper) as last_known"
            staging_step_6 += " from Staging." + table
            staging_step_6 += " where lenght(num_tdc) > 10"
            staging_step_6 += " and control = 'ok'"
            staging_step_6 += " group by producto, num_credito, num_cliente, num_tdc;"
            cursor.execute(staging_step_6)


            

            con.close()
            
        except Exception as e:
            print('Error: {}'.format(str(e)))    
        


    