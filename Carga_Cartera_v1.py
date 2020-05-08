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

warnings.simplefilter('error', UserWarning)

#funciones
#Constantes
filepattern01 = 'maecred'
filepattern02 = 'maesdos'
fileext = ".txt"
port = 3306
host = '192.168.0.15'
user = 'root'
password = 'Alb3rt-31nstein'
#host= '10.26.211.46'
#user= 'analitics'
#password= '2017YdwVCs51may2'
#user= 'c97635723'
#password= '9AJG7ae4gAE3av4a'
staging_table1 = 'tmp_maecred'
staging_table2 = 'tmp_maesdos'
table = 'Cuentas_tc.Cartera'
pasos_proceso = 6
proceso = 'Carga Cartera'


for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename1 = filepattern01 + '_' + cf.Nombre_mes((datetime.datetime.today() - datetime.timedelta(28)).strftime("%m")) + '_' + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y") + fileext
filename2 = filepattern02 + '_' + cf.Nombre_mes((datetime.datetime.today() - datetime.timedelta(28)).strftime("%m")) + '_' + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y") + fileext
filename = filename1 + " / " + filename2

if filename1 in files and filename2 in files:
    #print(filename1, " / ", filename2)
    try:
        paso = 1

        con = pymysql.connect(host = host, 
                          user = user, 
                          password = password, 
                          port = port,
                          autocommit=True,
                          local_infile=1)
        cursor = con.cursor()
        if cf.Validacion_archivo(cursor, filename1) or cf.Validacion_archivo(cursor, filename2):
            print('Archivo previamente cargado: ' + filename)
            con.close()
            sys.exit()
        
        staging_step_1a = "RENAME TABLE Cuentas_tc.Cartera TO Cuentas_tc.Cartera_" + (datetime.datetime.today() - datetime.timedelta(56)).strftime("%Y%m")
        cursor.execute(staging_step_1a)
        staging_step_1b = "CREATE TABLE if not exists Cuentas_tc.Cartera ( "
        staging_step_1b += "num_credito char(13) DEFAULT NULL,"
        staging_step_1b += " num_cliente varchar(10) DEFAULT NULL,"
        staging_step_1b += " producto varchar(10) DEFAULT NULL,"
        staging_step_1b += " sucursal char(4) DEFAULT NULL,"
        staging_step_1b += " status_cred char(2) DEFAULT NULL,"
        staging_step_1b += " linea_credito int(11) DEFAULT NULL,"
        staging_step_1b += " saldo_fin_mes double DEFAULT NULL,"
        staging_step_1b += " num_vencidos int(11) DEFAULT NULL,"
        staging_step_1b += " pago_min_pdo int(11) DEFAULT NULL,"
        staging_step_1b += " fecha_apertura char(11) DEFAULT NULL,"
        staging_step_1b += " f_primer_trxn varchar(11) DEFAULT NULL,"
        staging_step_1b += " primer_trxn varchar(11) DEFAULT NULL,"
        staging_step_1b += " Categoria_1 varchar(25) DEFAULT NULL,"
        staging_step_1b += " Categoria_2 varchar(15) DEFAULT NULL,"
        staging_step_1b += " Cluster_actividad varchar(15) DEFAULT NULL,"
        staging_step_1b += " fecha_cluster_act char(11) DEFAULT NULL,"
        staging_step_1b += " Cluster_inactividad varchar(15) DEFAULT NULL,"
        staging_step_1b += " fecha_cluster_inact char(11) DEFAULT NULL,"
        staging_step_1b += " KEY `cartera_fec` (fecha_apertura),"
        staging_step_1b += " KEY `cartera_cta` (num_credito),"
        staging_step_1b += " KEY `cartera_cte` (num_cliente)"
        staging_step_1b += ") ENGINE=InnoDB DEFAULT CHARSET=latin1;"
        cursor.execute(staging_step_1b)
        cf.logging_proceso(cursor,proceso + ': ' + filename ,pasos_proceso,paso,'Prepara tabla para carga')

        paso = 2
        cursor.execute('truncate table Staging.' + staging_table1 + ';')
        load_sql = "load data local infile '" + filename1 + "' into table Staging." + staging_table1
        load_sql += " fields terminated by '|' escaped by '' "
        load_sql += " lines terminated by '\n';"
        cursor.execute(load_sql)
        cf.logging_carga(cursor, filename1, staging_table1)

        cursor.execute('truncate table Staging.' + staging_table2 + ';')
        load_sql = "load data local infile '" + filename2 + "' into table Staging." + staging_table2
        load_sql += " fields terminated by '|' escaped by '' "
        load_sql += " lines terminated by '\n';"
        cursor.execute(load_sql)
        cf.logging_carga(cursor, filename2, staging_table2)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo solicitudes')
#Staging
        paso = 3
        staging_step_3a = "update Staging." + staging_table2 + " Set fecha = concat(substr(fecha,7,4), '-', substr(fecha,1,2), '-', substr(fecha,4,2))"
        staging_step_3a += ", control = 'ok'"
        staging_step_3a += "where control ='';"
        cursor.execute(staging_step_3a)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')

        paso = 4
        staging_step_4 = "insert into Historicos.Financieros select * from Staging." + staging_table2 + ";"
        cursor.execute(staging_step_4)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta información financiera en tabla histórica')

        paso = 5
        staging_step_5 = "insert into " + table + " (num_credito, num_cliente, producto, sucursal, status_cred, num_vencidos, saldo_fin_mes,"
        staging_step_5 += " fecha_apertura, pago_min_pdo, linea_credito, Categoria_2)"
        staging_step_5 += " select distinctrow a.num_credito,"
        staging_step_5 += " a.num_cte as num_cliente,"
        staging_step_5 += " case when substr(a.num_credito,1,2) = '60' then 'Clasica' when substr(a.num_credito,1,2) = '81' then 'Oro'"
        staging_step_5 += " when substr(a.num_credito,1,2) = '70' then 'Platinum' when substr(a.num_credito,1,2) = '66' then 'Básica'"
        staging_step_5 += " when substr(a.num_credito,1,2) = '78' then 'ADN' when substr(a.num_credito,1,2) = '61' then 'Reestructura' "
        staging_step_5 += " when substr(a.num_credito,1,2) = '63' then 'PP_12' when substr(a.num_credito,1,2) = '69' then 'PFB'"
        staging_step_5 += " when substr(a.num_credito,1,2) = '76' then 'PP_18' when substr(a.num_credito,1,2) = '77' then 'PP_24' "
        staging_step_5 += " when substr(a.num_credito,1,2) = '68' then 'Flexible' when substr(a.num_credito,1,2) = '85' then 'G.Coppel' "
        staging_step_5 += " else 'S/P' end as Prod,"
        staging_step_5 += " sucursal,"
        staging_step_5 += " estatus_credito,"
        staging_step_5 += " num_vencidos,"
        staging_step_5 += " b.saldos,"
        staging_step_5 += " fecha_apertura,"
        staging_step_5 += " b.pago_min_pdo,"
        staging_step_5 += " b.limite_credito,"
        staging_step_5 += " case when  saldos >= 0"
        staging_step_5 += " then case when (round(saldos/limite_credito,1)*100)>1000"
        staging_step_5 += " then 1000"
        staging_step_5 += " else round(saldos/limite_credito,1)*100"
        staging_step_5 += " end"
        staging_step_5 += " else case when (round(saldos/limite_credito,1)*100)<-1000"
        staging_step_5 += " then -1000"
        staging_step_5 += " else round(saldos/limite_credito,1)*100"
        staging_step_5 += " end"
        staging_step_5 += " end"
        staging_step_5 += " FROM Staging.tmp_maecred a, Staging.tmp_maesdos b"
        staging_step_5 += " where a.num_credito = b.num_credito;"
        cursor.execute(staging_step_5)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta informacion de cartera')

        paso = 6
        staging_step_6 = "update Cuentas_tc.Cartera a, Cuentas_tc.prim_actividad b set a.f_primer_trxn = b.f_primer_trxn, a.primer_trxn = b.primer_trxn"
        staging_step_6 += " where a.num_credito = b.num_credito;"
        cursor.execute(staging_step_6)
        cf.logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'actualiza fecha primera compra')
       
        print('Proceso de carga terminado: ' + filename)

        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
