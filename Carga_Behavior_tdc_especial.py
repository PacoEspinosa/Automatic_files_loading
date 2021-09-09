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
filepattern = 'OutBehavioral_'
fileext = ".txt"
staging_table_segmentado = 'tmpbehavioralv1'
staging_table = 'tmpbehavioral'
table = 'Historicos.OutBehavioral'
pasos_proceso = 3
proceso = 'Carga calificacion de comportamiento historico'
fecha_seguimiento = datetime.datetime.today().strftime("%Y-%m-%d")
dias_mes = '30'

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']


files_oneline = ['OutBehavioral_31012019.txt','OutBehavioral_28022019.txt','OutBehavioral_31032019.txt',
                 'OutBehavioral_30042019.txt','OutBehavioral_31052019.txt','OutBehavioral_30062019.txt',
                 'OutBehavioral_31072019.txt','OutBehavioral_30092019.txt','OutBehavioral_31102019.txt',
                 'OutBehavioral_31122019.txt','OutBehavioral_31012020.txt','OutBehavioral_31032020.txt',
                 'OutBehavioral_30042020.txt','OutBehavioral_31052020.txt','OutBehavioral_30062020.txt',
                 'OutBehavioral_31072020.txt','Outbehavioral_31082020.txt','OutBehavioral_30092020.txt',
                 'OutBehavioral_31102020.txt','OutBehavioral_30112020.txt','OutBehavioral_31122020.txt']

#files_oneline = []

for filename in files_oneline:
    fec_seguimiento = filename[18:22] + '-' + filename[16:18]
    #print(filename, '-', fec_seguimiento)   
    try:
        paso = 0
        con = pymysql.connect(host = host, 
                          user = user, 
                          password = password, 
                          port = port,
                          autocommit=True,
                          local_infile=1)
        cursor = con.cursor()
#        if cf.Validacion_archivo(cursor, filename):
#            print('Archivo previamente cargado: ' + filename)
#            con.close()
#        else:
        paso = 1
        load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
        load_sql += " fields terminated by '|' escaped by '' "
        load_sql += " lines terminated by '\n'"
        load_sql += " ;"
        #print(filename)
        cursor.execute('truncate table Staging.' + staging_table + ';')
        cursor.execute(load_sql)
        cf.logging_carga(cursor, filename, staging_table)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa archivo OutBehavioral')
#Staging
        paso = 2
        staging_step_2b = "insert ignore into " + table 
        staging_step_2b += " select distinct '" + fec_seguimiento + "' as fec_seguimiento,"
        staging_step_2b += " substr(registro,4,12) as num_credito, trim(substr(registro,31,12)) as designacion,"
        staging_step_2b += " abs(substr(registro,43,5)) as calificacion, trim(substr(registro,55,9)) as riesgo"
        staging_step_2b += " from Staging." + staging_table + ";"
        cursor.execute(staging_step_2b)
        cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta registros actuales')


        print('Proceso de carga terminado: ' + filename)
       

        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

con = pymysql.connect(host = host, 
                  user = user, 
                  password = password, 
                  port = port,
                  autocommit=True,
                  local_infile=1)
cursor = con.cursor()

paso = 3
staging_step_3 = "create table visa_cashwithdrawal.comportamiento as select a.num_cliente, b.num_credito, "
staging_step_3 += " fec_seguimiento, Calificación"
staging_step_3 += " from Cuentas_tc.Cartera_202012 a, Historicos.OutBehavioral b"
staging_step_3 += " where a.num_credito = b.num_credito"
staging_step_3 += " and producto = 'Clasica';"
cursor.execute(staging_step_3)
cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'crea base para exportar')

paso = 4
staging_step_4 = "select 'num_cliente' num_cliente, 'num_credito' num_credito,"
staging_step_4 += " 'fec_seguimiento' fec_seguimiento, 'Calificación' Calificación"
staging_step_4 += " union select num_cliente, num_credito, fec_seguimiento, Calificación"
staging_step_4 += " from visa_cashwithdrawal.comportamiento"
staging_step_4 += " INTO OUTFILE '//var//lib//mysql-files//cash_adv_comportamiento.txt'"
staging_step_4 += " FIELDS TERMINATED BY '|'"
staging_step_4 += " LINES TERMINATED BY '\r\n';"
cursor.execute(staging_step_4)
cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'exporta base para envio')

con.close()

