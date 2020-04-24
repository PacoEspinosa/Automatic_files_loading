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

#Variables
path = os.getcwd() #obtiene el directorio de trabajo actual
files = []

warnings.simplefilter('error', UserWarning)

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
filepattern = 'solic'
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
staging_table = 'tmp_solicitudes'
table = 'Solicitudes.Solicitudes_2017'
pasos_proceso = 7
proceso = 'Carga transacciones'


for r, d, f in os.walk(path):
    for file in f:
        files.append(file)

filename = filepattern + datetime.datetime.today().strftime("%Y%m%d") + fileext
if filename in files:
    load_sql = "load data local infile '" + filename + "' into table Staging." + staging_table
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
        cursor.execute('truncate table Staging.' + staging_table + ';')
        cursor.execute(load_sql)
        logging_carga(cursor, filename, staging_table)
        logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Carga archivo solicitudes')
#Staging
        paso = 2
        staging_step_1a = "update Staging." + staging_table + " Set FECHASOL = concat(substr(FECHASOL,7,4), '-', substr(FECHASOL,1,2), '-', substr(FECHASOL,4,2)),"
        staging_step_1a += "where length(FECHASOL) = 10;"
        cursor.execute(staging_step_1a)
        staging_step_1b = "update Staging." + staging_table + " Set FECHANAC = concat(substr(FECHANAC,7,4), '-', substr(FECHANAC,1,2), '-', substr(FECHANAC,4,2)),"
        staging_step_1b += "where length(FECHANAC) = 10;"
        cursor.execute(staging_step_1b)
        staging_step_1c = "update Staging." + staging_table + " Set FECHARESP = concat(substr(FECHASOL,7,4), '-', substr(FECHARESP,1,2), '-', substr(FECHARESP,4,2)),"
        staging_step_1c += "where length(FECHARESP) = 10;"
        cursor.execute(staging_step_1c)
        staging_step_1d = "update Staging." + staging_table + " Set FECHA_APERT = concat(substr(FECHA_APERT,7,4), '-', substr(FECHA_APERT,1,2), '-', substr(FECHA_APERT,4,2)),"
        staging_step_1d += "where length(FECHA_APERT) = 10;"
        cursor.execute(staging_step_1d)
        logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza formato de fechas')

        paso = 3
        staging_step_2a = "insert into Staging.tmp_solicitudes_rvw select * from Staging.tmp_solicitudes where length(FECHASOL) <> 10;"
        cursor.execute(staging_step_2a)
        staging_step_2b = "delete from Staging." + staging_table + " where length(FECHASOL) <> 10;"
        cursor.execute(staging_step_2b)
        logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Elimina registros con fechas incorrectas')

        paso = 4
        staging_step_3 = "update " + table + " a, Staging." + staging_table + " b set a.statussol = b.statussol,"
        staging_step_3 += " a.causa = b.causa,"
        staging_step_3 += " a.status = b.status,"
        staging_step_3 += " a.fecha_apert = b.fecha_apert,"
        staging_step_3 += " a.fecharesp=b.fecharesp," 
        staging_step_3 += " a.TEL_CEL=b.TEL_CEL"
        staging_step_3 += " where a.numsolicitud = b.numsolicitud;"
        cursor.execute(staging_step_3)
        logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'Actualiza status solicitudes existentes')

        paso = 5
        staging_step_4 = "insert into " + table + " select"
        staging_step_4 += " a.NUMSOLICITUD,"
        staging_step_4 += " a.NUMCTE,"
        staging_step_4 += " a.NUMCTECOPPEL,"
        staging_step_4 += " a.SUCURSAL,"
        staging_step_4 += " a.APPATERNO,"
        staging_step_4 += " a.APMATERNO,"
        staging_step_4 += " a.NOMBRE1,"
        staging_step_4 += " a.RFC,"
        staging_step_4 += " a.FECHANAC,"
        staging_step_4 += " a.CLACIUCOBR,"
        staging_step_4 += " a.CLAEDOCOBR,"
        staging_step_4 += " a.CODPOSTAL,"
        staging_step_4 += " a.TELEFONO,"
        staging_step_4 += " a.ESTADO,"
        staging_step_4 += " a.LOCALIDAD,"
        staging_step_4 += " a.STATUSSOL,"
        staging_step_4 += " a.FECHASOL,"
        staging_step_4 += " a.NUMPRODUCTO,"
        staging_step_4 += " a.RESPUESTA,"
        staging_step_4 += " a.FECHARESP,"
        staging_step_4 += " a.EJECUTIVO,"
        staging_step_4 += " a.INGRESOMENSUAL,"
        staging_step_4 += " a.INGRESOSMB,"
        staging_step_4 += " a.LINCRED,"
        staging_step_4 += " a.EFICPONDERADA,"
        staging_step_4 += " a.MESES,"
        staging_step_4 += " a.SITESP,"
        staging_step_4 += " a.CAUSASITESP,"
        staging_step_4 += " a.TIPOCLIENTE,"
        staging_step_4 += " a.FILTROCLIENTE,"
        staging_step_4 += " a.SALDOROPA,"
        staging_step_4 += " a.SALDOMUEBLES,"
        staging_step_4 += " a.SALDOPRESTAMO,"
        staging_step_4 += " a.LINEATIENDA,"
        staging_step_4 += " a.BCSCORE,"
        staging_step_4 += " a.CAUSA,"
        staging_step_4 += " a.STATUS,"
        staging_step_4 += " a.COMPROMISOS,"
        staging_step_4 += " a.FECHA_APERT,"
        staging_step_4 += " a.EMAIL,"
        staging_step_4 += " a.TEL_OFI,"
        staging_step_4 += " a.TEL_CEL,"
        staging_step_4 += " a.FUENTE,"
        staging_step_4 += " a.RESPUESTACC,"
        staging_step_4 += " a.SEXO,"
        staging_step_4 += " a.ESTADO_CIVIL,"
        staging_step_4 += " a.TMPO_EDO_CIV_ACT,"
        staging_step_4 += " a.TIPO_RESIDENCIA,"
        staging_step_4 += " a.TMPO_DOM_ACT,"
        staging_step_4 += " a.OCUPACION,"
        staging_step_4 += " a.TMPO_OCUP_ACT,"
        staging_step_4 += " a.TMPO_OCUP_ANT,"
        staging_step_4 += " a.EDAD,"
        staging_step_4 += " a.DEPEND_ECON,"
        staging_step_4 += " a.SEGURO_POPULAR,"
        staging_step_4 += " a.ESCOLARIDAD,"
        staging_step_4 += " a.HAB_DOMIC,"
        staging_step_4 += " a.BC_1,"
        staging_step_4 += " a.PUNTUAL_BC_1,"
        staging_step_4 += " a.BC_101,"
        staging_step_4 += " a.PUNTUAL_BC_101,"
        staging_step_4 += " a.BC_117,"
        staging_step_4 += " a.PUNTUAL_BC_117,"
        staging_step_4 += " a.BC_119,"
        staging_step_4 += " a.PUNTUAL_BC_119,"
        staging_step_4 += " a.BC_20,"
        staging_step_4 += " a.PUNTUAL_BC_20,"
        staging_step_4 += " a.BC_421,"
        staging_step_4 += " a.PUNTUAL_BC_421,"
        staging_step_4 += " a.BC_85,"
        staging_step_4 += " a.PUNTUAL_BC_85,"
        staging_step_4 += " a.BC_93,"
        staging_step_4 += " a.PUNTUAL_BC_93,"
        staging_step_4 += " a.CALC_PCT_SALDO_LINEA,"
        staging_step_4 += " a.MESES_HISTORIA,"
        staging_step_4 += " a.PUNTUAL_MESES_HISTORIA,"
        staging_step_4 += " a.SITUACION_PAGO,"
        staging_step_4 += " a.PUNTUAL_SITUACION_PAGO,"
        staging_step_4 += " a.RATIO_SALDO_CREDIT_LIMIT,"
        staging_step_4 += " a.PUNTUAL_RATIO_SALDO_CREDIT_LIMIT,"
        staging_step_4 += " a.SECCION1,"
        staging_step_4 += " a.SECCION2,"
        staging_step_4 += " a.SUMASCORING,"
        staging_step_4 += " a.ABONO_MUEBLES,"
        staging_step_4 += " a.ABONO_ROPA,"
        staging_step_4 += " a.ABONO_PRESTAMOS,"
        staging_step_4 += " a.COMPROMISOS_MENSUALES,"
        staging_step_4 += " a.EVALUA_CC,"
        staging_step_4 += " a.VI_TMPO_EDO_CIVIL,"
        staging_step_4 += " a.RAN_VI_TMPO_EDO_CIVIL,"
        staging_step_4 += " a.VI_MHIST_CTENVO,"
        staging_step_4 += " a.RAN_VI_MHIST_CTENVO,"
        staging_step_4 += " a.VI_SDOLINEA_CTENVO,"
        staging_step_4 += " a.RAN_SDOLINEA_CTENVO,"
        staging_step_4 += " a.VI_SITPAGO_CTENVO,"
        staging_step_4 += " a.RAN_SITPAGO_CTENVO,"
        staging_step_4 += " a.REGION_COBRANZA,"
        staging_step_4 += " a.MES_ULT_CONSULTA,"
        staging_step_4 += " a.GRUPO,"
        staging_step_4 += " a.Control1,"
        staging_step_4 += " a.Control2,"
        staging_step_4 += " a.Control3,"
        staging_step_4 += " a.Control4"
        staging_step_4 += " from Staging.tmp_solicitudes a left join Solicitudes.Solicitudes_2017 b"
        staging_step_4 += " on a.numsolicitud = b.numsolicitud"
        staging_step_4 += " where b.numsolicitud is null and a.tel_cel not in ('BANCO','TIENDA');"
        cursor.execute(staging_step_4)
        logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso,paso,'inserta solicitudes nuevas')

        paso = 6
        staging_step_5 = "insert into Datos_generales.Domicilio (num_cliente) select distinct a.numcte"
        staging_step_5 += " from Staging.tmp_solicitudes a left join Datos_generales.Domicilio b"
        staging_step_5 += " on a.NUMCTE = b.Num_cliente"
        staging_step_5 += " where b.Num_cliente is null;"
        cursor.execute(staging_step_5)
        logging_proceso(cursor, proceso + ': ' + filename, pasos_proceso, paso, 'inserta en domicilios nuevos')
       
        paso = 7
        staging_step_6 = "update Datos_generales.Domicilio a, Staging.tmp_solicitudes b set a.Calle = b.CALLE,"
        staging_step_6 += " a.numext = b.numext,"
        staging_step_6 += " a.numint = b.numint,"
        staging_step_6 += " a.Colonia = b.colonia,"
        staging_step_6 += " a.CodPostal = b.codpostal,"
        staging_step_6 += " a.EntreCalles = b.entrecalles,"
        staging_step_6 += " a.Estado = b.estado,"
        staging_step_6 += " a.Localidad = b.localidad,"
        staging_step_6 += " a.Observaciones = b.observaciones,"
        staging_step_6 += " a.UltModificacion = b.FECHASOL"
        staging_step_6 += " where a.Num_cliente = b.numcte;"
        cursor.execute(staging_step_6)
        logging_proceso(cursor,proceso + ': ' + filename,pasos_proceso, paso,'Actualiza domicilios con existentes')


        print('Proceso de carga terminado: ' + filename)

        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)) + ' Paso:' + str(paso))    
else:
    print('No se localizó el archivo: ' + filename)
