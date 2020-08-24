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
filepattern = 'Credisol_Det_'
fileext = ".txt"
staging_table = 'tmp_credisolucion'
table = 'Cuentas_tc.Credisolucion'
historic_table = 'Historicos.Credisolucion'
general_table = 'Cuentas_tc.Credisolucion_general'
pasos_proceso = 7
proceso = 'Carga Credisol'
fecha_seguimiento = (datetime.datetime.today() - datetime.timedelta(28)).strftime("%Y-%m")

#carga configuracion
exec(open("config.py").read())
user = config['Database_Config']['usuario']
password = config['Database_Config']['contrasena'] 
host = config['Database_Config']['servidor'] 
port = config['Database_Config']['puerto']

files = cf.listado_archivos(path, filepattern)

#filename = filepattern + '05' + datetime.datetime.today().strftime("%m%Y") + fileext
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
            cursor.execute('truncate table Staging.' + staging_table + ';')
            load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
            load_sql += " fields terminated by '|' escaped by '' "
            load_sql += " lines terminated by '\n'"
            load_sql += " ignore 1 lines;"
            #print(filename)
            cursor.execute(load_sql)
            cf.logging_carga(cursor, filename, staging_table)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo credisoluciones')
    #Staging
            paso = 2
            staging_step_2a = "update Staging." + staging_table + " Set fecha_contratacion = concat(substr(fecha_contratacion,7,4), '-', substr(fecha_contratacion,1,2), '-', substr(fecha_contratacion,4,2))"
            staging_step_2a += ", control = 'ok'"
            staging_step_2a += "where length(fecha_contratacion) = 10;"
            cursor.execute(staging_step_2a)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')
    
            paso = 3
            staging_step_3a = "Truncate table " + table + ";"
            cursor.execute(staging_step_3a)
            staging_step_3b = "insert into " + table
            staging_step_3b += " select num_credito, Num_Cliente, Num_Credisoluciones, Fecha_Contratacion, Sucursal, Nombre_Promocion,"
            staging_step_3b += " Tasa, Monto_Contratado, Comision_disposicion, Plazo, Saldo_Insoluto, Capital_Insoluto,"
            staging_step_3b += " Interes_pagar, Iva_Pagar, Estatus_Credisolucion, Motivo, Mensualidades_Pagar,"
            staging_step_3b += " Monto_mensualidad, Intereses_Cargados_Acumulados, Iva_Cargados"
            staging_step_3b += " from Staging." + staging_table 
            staging_step_3b += " where Control  = 'ok';"
            cursor.execute(staging_step_3b)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros mes actual')
    
            paso = 4
            staging_step_4 = "insert into " + historic_table 
            staging_step_4 += " select Num_Credito, Num_Cliente, Num_credisoluciones, " + fecha_seguimiento + " as Fecha_seguimiento,"
            staging_step_4 += " Saldo_Insoluto, Capital_Insoluto, Estatus_Credisolucion, Motivo, Mensualidades_pagar"
            staging_step_4 += " from Staging." + staging_table + ";"
            cursor.execute(staging_step_4)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros base histórica')
    
            paso = 5
            staging_step_5 = "insert into " + general_table + " select"
            staging_step_5 += " a.num_credito,"
            staging_step_5 += " a.Num_Cliente,"
            staging_step_5 += " a.Num_Credisoluciones,"
            staging_step_5 += " a.Fecha_Contratacion,"
            staging_step_5 += " null as Fecha_ult_seg,"
            staging_step_5 += " a.Sucursal,"
            staging_step_5 += " a.Nombre_Promocion,"
            staging_step_5 += " a.Tasa,"
            staging_step_5 += " a.Monto_Contratado,"
            staging_step_5 += " a.Plazo,"
            staging_step_5 += " a.Estatus_Credisolucion,"
            staging_step_5 += " a.Motivo,"
            staging_step_5 += " a.Monto_mensualidad"
            staging_step_5 += " from Staging." + staging_table + " a left join " + general_table + " b"
            staging_step_5 += " on a.num_credisoluciones = b.num_credisoluciones"
            staging_step_5 += " where Control  = 'ok' and b.num_credisoluciones is null;"
            cursor.execute(staging_step_5)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Inserta registros base general')
    
            paso = 6
            staging_step_6 = "update Staging." + staging_table + " a, " + general_table + " b"
            staging_step_6 += " set b.Estatus_Credisolucion = a.Estatus_Credisolucion"
            staging_step_6 += ", b.Motivo = a.Motivo"
            staging_step_6 += " where a.num_credisoluciones = b.num_credisoluciones"
            staging_step_6 += " and Control  = 'ok';"
            cursor.execute(staging_step_6)
            cf.logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'Actualiza status en base general')
           
            paso = 7
            staging_step_7 = "update " + general_table + " a left join Staging." + staging_table + " b"
            staging_step_7 += " on a.num_credisoluciones = b.num_credisoluciones"
            staging_step_7 += " set a.Fecha_ult_seg = " + fecha_seguimiento
            staging_step_7 += " where b.num_credisoluciones is null"
            staging_step_7 += " and Control  = 'ok';"
            cursor.execute(staging_step_7)
            cf.logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Actualiza fecha de seguimiento')
    
    
            print('Proceso de carga terminado: ' + filename)
    
            con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    

if files == []:
    print('No se localizaron archivos de carga')
