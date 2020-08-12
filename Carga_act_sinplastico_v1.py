#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:34:44 2019
subject: Proceso de carga historica, donde se conoce las fechas disponibles
 y los archivos a cargar se encuentran en una carpeta especifica.
@author: francisco
"""
#Librerias
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
filepattern = 'cuentas_activas_sin_plastico_'
fileext = ".txt"
staging_table = 'tmp_act_sin_plastico'
table = 'Campañas.Act_sin_plastico'
table_historico = 'Campañas.Historico_act_sin_plastico'
pasos_proceso = 6
proceso = 'Carga Activas sin plastico'
fecha_seguimiento = datetime.datetime.today().strftime("%Y-%m")

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)

#filename = filepattern + '03' + datetime.datetime.today().strftime("%m%y") + fileext
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
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Importa archivo mensual')
    #Staging
            paso = 2
            staging_step_2a = "update Staging." + staging_table + " Set Fec_activacion = concat(substr(Fec_activacion,7,4), '-', substr(Fec_activacion,1,2), '-', substr(Fec_activacion,4,2))"
            staging_step_2a += "where length(Fec_activacion) = 10;"
            cursor.execute(staging_step_2a)
            staging_step_2b = "update Staging." + staging_table + " Set Fec_ult_mov = concat(substr(Fec_ult_mov,7,4), '-', substr(Fec_ult_mov,1,2), '-', substr(Fec_ult_mov,4,2))"
            staging_step_2b += "where length(Fec_ult_mov) = 10;"
            cursor.execute(staging_step_2b)
            staging_step_2c = "update Staging." + staging_table + " Set Fec_vencimiento = concat(substr(Fec_vencimiento,7,4), '-', substr(Fec_vencimiento,1,2), '-', substr(Fec_vencimiento,4,2))"
            staging_step_2c += "where length(Fec_vencimiento) = 10;"
            cursor.execute(staging_step_2c)
            staging_step_2d = "update Staging." + staging_table + " Set Fec_ult_mod = concat(substr(Fec_ult_mod,7,4), '-', substr(Fec_ult_mod,1,2), '-', substr(Fec_ult_mod,4,2))"
            staging_step_2d += "where length(Fec_ult_mod) = 10;"
            cursor.execute(staging_step_2d)
            staging_step_2e = "update Staging." + staging_table + " Set Fec_asignacion = concat(substr(Fec_asignacion,7,4), '-', substr(Fec_asignacion,1,2), '-', substr(Fec_asignacion,4,2))"
            staging_step_2e += "where length(Fec_asignacion) = 10;"
            cursor.execute(staging_step_2e)
            staging_step_2f = "update Staging." + staging_table + " Set Control = 'ok'"
            staging_step_2f += "where (length(Fec_asignacion) = 10"
            staging_step_2f += " or length(Fec_activacion) = 10"
            staging_step_2f += " or length(Fec_ult_mov) = 10"
            staging_step_2f += " or length(Fec_vencimiento) = 10"
            staging_step_2f += " or length(Fec_ult_mod) = 10)"
            staging_step_2f += " and Control is null;"
            cursor.execute(staging_step_2f)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_3a = "truncate table " + table + ";"
            cursor.execute(staging_step_3a)
            staging_step_3b = "insert into " + table + " (Num_credito," 
            staging_step_3b += " Num_cliente,"
            staging_step_3b += " Fec_activacion,"
            staging_step_3b += " Fec_ult_mov,"
            staging_step_3b += " Fec_vencimiento,"
            staging_step_3b += " Estatus_tar,"
            staging_step_3b += " Secuencia,"
            staging_step_3b += " Entidad_fed,"
            staging_step_3b += " Fec_ult_mod,"
            staging_step_3b += " Usuario_ult_mod,"
            staging_step_3b += " Fec_asignacion,"
            staging_step_3b += " Saldo,"
            staging_step_3b += " Email,"
            staging_step_3b += " Celular,"
            staging_step_3b += " Usuario_entrego)"
            staging_step_3b += " select Num_credito,"
            staging_step_3b += " Num_cliente,"
            staging_step_3b += " Fec_activacion,"
            staging_step_3b += " Fec_ult_mov,"
            staging_step_3b += " Fec_vencimiento,"
            staging_step_3b += " Estatus_tar,"
            staging_step_3b += " Secuencia,"
            staging_step_3b += " Entidad_fed,"
            staging_step_3b += " Fec_ult_mod,"
            staging_step_3b += " Usuario_ult_mod,"
            staging_step_3b += " Fec_asignacion,"
            staging_step_3b += " Saldo,"
            staging_step_3b += " Email,"
            staging_step_3b += " Celular,"
            staging_step_3b += " Usuario_entrego"
            staging_step_3b += " from Staging." + staging_table
            staging_step_3b += " where Control = 'ok';"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Integra a base del mes corriente')
    
            paso = 4
            staging_step_4 = "insert into " + table_historico + " select Num_credito," 
            staging_step_4 += " Num_cliente,"
            staging_step_4 += " Fec_activacion,"
            staging_step_4 += " Fec_ult_mov,"
            staging_step_4 += " Fec_vencimiento," 
            staging_step_4 += " Estatus_tar,"
            staging_step_4 += " Secuencia,"
            staging_step_4 += " Entidad_fed,"
            staging_step_4 += " Fec_ult_mod,"
            staging_step_4 += " Usuario_ult_mod,"
            staging_step_4 += " Fec_asignacion,"
            staging_step_4 += " Saldo,"
            staging_step_4 += " Email,"
            staging_step_4 += " Celular,"
            staging_step_4 += " Usuario_entrego,"
            staging_step_4 += " '" + fecha_seguimiento + "'"
            staging_step_4 += " from Staging." + staging_table
            staging_step_4 += " where Control = 'ok';"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Agrega a base histórica')
    
            paso = 5
            staging_step_5a = "drop table if exists Trabajos_prueba.Frecuencia_acumulada_act_sin_plas;"
            cursor.execute(staging_step_5a)
            staging_step_5b = "create table Trabajos_prueba.Frecuencia_acumulada_act_sin_plas as select num_credito, estatus_tar, count(*) as regs "
            staging_step_5b += " from " + table_historico
            staging_step_5b += " group by num_credito, estatus_tar;"
            cursor.execute(staging_step_5b)
            staging_step_5c = "create index tmp_frec_acum_act_sin_plas on Trabajos_prueba.Frecuencia_acumulada_act_sin_plas (num_credito asc);"
            cursor.execute(staging_step_5c)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Calcula frecuencia acumulada de los registros')
    
            paso = 6
            staging_step_6 = "update " + table + " a, Trabajos_prueba.Frecuencia_acumulada_act_sin_plas b set Frec_acumulada = regs"
            staging_step_6 += " where (a.num_credito = b.num_credito and a.estatus_tar = b.estatus_tar);"
            cursor.execute(staging_step_6)
            cf.logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'Actualiza frecuencia acumulada')
           
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

if files == []:
    print('No se localizaron archivos de carga')
