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
filepattern01 = 'maecred'
filepattern02 = 'maesdos'
fileext = "_reestructura.txt"
staging_table1 = 'tmp_maecred'
staging_table2 = 'tmp_maesdos'
table = 'Cuentas_tc.Cartera_PP'
pasos_proceso = 4
proceso = 'Carga Cartera PP'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename1 = filepattern01 + '_' + cf.Nombre_mes((datetime.datetime.today() - datetime.timedelta(28)).strftime("%m")) + '_' + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y") + fileext
filename2 = filepattern02 + '_' + cf.Nombre_mes((datetime.datetime.today() - datetime.timedelta(28)).strftime("%m")) + '_' + (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y") + fileext
filename = filename1 + " / " + filename2

if filename1 in files and filename2 in files:
    #print(filename1, " / ", filename2)
    try:
        paso = 0
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
        else:
            paso = 1
            staging_step_1a = "RENAME TABLE " + table + " TO " + table + "_" + (datetime.datetime.today() - datetime.timedelta(56)).strftime("%Y%m")
            cursor.execute(staging_step_1a)
            staging_step_1b = "CREATE TABLE if not exists "+ table + " ( "
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
            staging_step_1b += " Categoria_1 varchar(25) DEFAULT NULL,"
            staging_step_1b += " Categoria_2 varchar(15) DEFAULT NULL,"
            staging_step_1b += " Cluster_actividad varchar(15) DEFAULT NULL,"
            staging_step_1b += " fecha_cluster_act char(11) DEFAULT NULL,"
            staging_step_1b += " Cluster_inactividad varchar(15) DEFAULT NULL,"
            staging_step_1b += " fecha_cluster_inact char(11) DEFAULT NULL,"
            staging_step_1b += " KEY `carterapp_fec` (fecha_apertura),"
            staging_step_1b += " KEY `carterapp_cta` (num_credito),"
            staging_step_1b += " KEY `carterapp_cte` (num_cliente)"
            staging_step_1b += ") ENGINE=InnoDB DEFAULT CHARSET=latin1;"
            cursor.execute(staging_step_1b)
            cf.logging_proceso(cursor,proceso + ': ' + filename ,pasos_proceso,paso,'Prepara tabla para carga')
    
            paso = 2
            cursor.execute('truncate table Staging.' + staging_table1 + ';')
            load_sql1 = "load data local infile '" + filename1 + "' into table Staging." + staging_table1
            load_sql1 += " fields terminated by '|' escaped by '' "
            load_sql1 += " lines terminated by '\n';"
            cursor.execute(load_sql1)
            cf.logging_carga(cursor, filename1, staging_table1)
    
            cursor.execute('truncate table Staging.' + staging_table2 + ';')
            load_sql2 = "load data local infile '" + filename2 + "' into table Staging." + staging_table2
            load_sql2 += " fields terminated by '|' escaped by '' "
            load_sql2 += " lines terminated by '\n';"
            cursor.execute(load_sql2)
            cf.logging_carga(cursor, filename2, staging_table2)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo solicitudes')
    #Staging
            paso = 3
            staging_step_3a = "update Staging." + staging_table1 + " Set fecha_apertura = concat(substr(fecha_apertura,7,4), '-', substr(fecha_apertura,1,2), '-', substr(fecha_apertura,4,2))"
            staging_step_3a += ", control = 'ok'"
            staging_step_3a += " where control is null;"
            cursor.execute(staging_step_3a)
            staging_step_3b = "update Staging." + staging_table2 + " Set fecha = concat(substr(fecha,7,4), '-', substr(fecha,1,2), '-', substr(fecha,4,2))"
            staging_step_3b += ", control = 'ok'"
            staging_step_3b += " where control ='';"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 4
            staging_step_4 = "insert into " + table + " (num_credito, num_cliente, producto, sucursal, status_cred, num_vencidos, saldo_fin_mes,"
            staging_step_4 += " fecha_apertura, pago_min_pdo, linea_credito, Categoria_2)"
            staging_step_4 += " select distinctrow a.num_credito,"
            staging_step_4 += " a.num_cte as num_cliente,"
            staging_step_4 += " case when substr(a.num_credito,1,2) = '60' then 'Clasica' when substr(a.num_credito,1,2) = '81' then 'Oro'"
            staging_step_4 += " when substr(a.num_credito,1,2) = '70' then 'Platinum' when substr(a.num_credito,1,2) = '66' then 'Básica'"
            staging_step_4 += " when substr(a.num_credito,1,2) = '78' then 'ADN' when substr(a.num_credito,1,2) = '61' then 'Reestructura' "
            staging_step_4 += " when substr(a.num_credito,1,2) = '63' then 'PP_12' when substr(a.num_credito,1,2) = '69' then 'PFB'"
            staging_step_4 += " when substr(a.num_credito,1,2) = '76' then 'PP_18' when substr(a.num_credito,1,2) = '77' then 'PP_24' "
            staging_step_4 += " when substr(a.num_credito,1,2) = '68' then 'Flexible' when substr(a.num_credito,1,2) = '85' then 'G.Coppel' "
            staging_step_4 += " else 'S/P' end as Prod,"
            staging_step_4 += " sucursal,"
            staging_step_4 += " estatus_credito,"
            staging_step_4 += " clase1,"
            staging_step_4 += " saldos,"
            staging_step_4 += " fecha_apertura,"
            staging_step_4 += " b.pago_min_pdo,"
            staging_step_4 += " b.limite_credito,"
            staging_step_4 += " case when  saldos >= 0"
            staging_step_4 += " then case when (round(saldos/limite_credito,1)*100)>1000"
            staging_step_4 += " then 1000"
            staging_step_4 += " else round(saldos/limite_credito,1)*100"
            staging_step_4 += " end"
            staging_step_4 += " else case when (round(saldos/limite_credito,1)*100)<-1000"
            staging_step_4 += " then -1000"
            staging_step_4 += " else round(saldos/limite_credito,1)*100"
            staging_step_4 += " end"
            staging_step_4 += " end"
            staging_step_4 += " FROM Staging.tmp_maecred a, Staging.tmp_maesdos b"
            staging_step_4 += " where a.num_credito = b.num_credito;"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta informacion de cartera pp')
    
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
