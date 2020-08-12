# -*- coding: utf-8 -*-
"""
Created on Thu May  7 16:18:11 2020

@author: Francisco Espinosa
"""

def logging_carga(cursor_con, filename, staging_table):
    mysql_log_load = "insert into Operacion_datamart.Importacion "
    mysql_log_load += " select curdate() as fecha, '" + filename + "' as nombre_archivo, user() as Usuario, count(*) as Registros, 0 as reproceso from Staging." + staging_table + "; ";
    cursor_con.execute(mysql_log_load)
    
def logging_proceso(cursor_con, process, total_steps, step, descripcion):
    Etapa = str(step) + "/" + str(total_steps) + ") " + descripcion
    mysql_log_task = "insert into Operacion_datamart.Logs_procesos (fecha, Proceso, Etapa) "
    mysql_log_task += " select now() as fecha, '" + process + "' , '" + Etapa + "';"
    #print(mysql_log_task)
    cursor_con.execute(mysql_log_task)

def Validacion_archivo (cursor_con, filename):
    stmt = "select * from Operacion_datamart.Importacion where Nombre_archivo = '" + filename + "' and Reproceso = 0;"
    cursor_con.execute(stmt)
    result = cursor_con.fetchone()
    if result:
        return True
    else:
        return False    

def listado_archivos (path, filepattern):
    import os
    import fnmatch
    files = []
    for file in os.listdir(path):
        if fnmatch.fnmatch(file, (filepattern + '*')):
            #print(file)
            files.append(file)
    return files

def Nombre_mes (mes):
    switcher = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre"
    }
    return switcher.get(int(mes), "")

