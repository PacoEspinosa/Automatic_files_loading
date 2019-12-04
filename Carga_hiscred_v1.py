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
    
def logging_proceso(cursor_con, process, total_steps, step):
    mysql_log_task = "insert into Operacion_datamart.Importacion "
    mysql_log_task += " select curdate() as fecha, '" + filename + "' as nombre_archivo, user() as Usuario, count(*) as Registros from Staging." + staging_table + "; ";
    cursor_con.execute(mysql_log_task)
    

#Constantes
date1 = '2016-01-01' 
date2 = '2016-12-31'
filepattern = 'hiscred'
fileext = ""
host= '10.26.211.46'
user= 'analitics'
password= '2017YdwVCs51may2'
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
                              autocommit=True,
                              local_infile=1)
            cursor = con.cursor()
            cursor.execute("truncate table Staging." + table + ';')
            cursor.execute(load_sql)
#Staging
            staging_step_1 = "update Staging.tmphiscred Set fechaoper = concat(substr(fechaoper,7,4), "-", substr(fechaoper,1,2), "-", substr(fechaoper,4,2)),"
            staging_step_1 += "control = 'ok' where (control = '' or control is null);"
            cursor.execute(staging_step_1)

            staging_step_2 = "insert into Historicos.Intereses select fechaoper, num_credito, Monto, descripcion, transaccion"
            staging_step_2 += " from Staging.tmphiscred"
            staging_step_2 += " where transaccion in ('6940','4201','6608')"
            staging_step_2 += " and control = 'ok';"
            cursor.execute(staging_step_2)
    
	producto varchar(5), 
	num_credito varchar(13), 
	num_cliente varchar(10), 
	sucursal varchar(5), 
	folio_suc varchar(16), 
	num_tdc varchar(16), 
	monto DOUBLE, 
	tipo_trxn varchar(30), 
	transaccion varchar(5), 
	contable varchar(80), 
	fechaoper varchar(11),
    efecto_saldo int,

            staging_step_3 = "insert into Transacciones.Transacciones_TDC_2016 select producto, num_credito, num_cliente, sucursal,
            staging_step_3 += " folio_suc, num_tdc, monto, tipo_trxn, transaccion, contable, fechaoper, efecto_saldo"
            staging_step_3 += " from Staging.tmphiscred"
            staging_step_3 += " where transaccion in ('6940','4201','6608')"
            staging_step_3 += " and control = 'ok';"
            cursor.execute(staging_step_3)
            

            con.close()
            
        except Exception as e:
            print('Error: {}'.format(str(e)))    
        


    