# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 14:45:22 2021

@author: fespinosa
"""

import sqlite3
import pandas as pd
import os
import Complement_functions as cf

database_path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\DWH\\'
filename_path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\tmp\\Carga\\'
filename_path_bck = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\tmp\\Carga\\historico\\'

tables = ['hiscred_pres_flex','hiscred_ADN','Hiscred_GC','Hiscred_Oro','hiscred_pres','hiscred']
dates = ['202101','202102','202103','202104','202105','202106','202107','202108']

for fecha in dates:
    database_filename = 'trxn_diarias_' + fecha + '.db'
    conn = sqlite3.connect(database_path + database_filename)
    cursorObj = conn.cursor()
    """
    updt_fecha = "Select  case when abs(producto) = 7700 then 'PP 24M'  when abs(producto) = 7600 then 'PP 18M'"
    updt_fecha += " when abs(producto) = 6300 then 'PP 12M' when abs(producto) = 6800 then 'P Digital' end as prod, "
    updt_fecha += " sum(monto) as vol, count(*) as regs, count(distinct num_cuenta) as ctas"
    updt_fecha += " from hiscred_pres"
    updt_fecha += " where abs(transaccion) in (7461)"
    updt_fecha += " and abs(secuencia) = 1"
    updt_fecha += " and abs(producto) in (7700, 7600, 6300, 6800)"
    updt_fecha += " group by  case when abs(producto) = 7700 then 'PP 24M'  when abs(producto) = 7600 then 'PP 18M'"
    updt_fecha += " when abs(producto) = 6300 then 'PP 12M' when abs(producto) = 6800 then 'P Digital' end "
    updt_fecha += ";"
    cursorObj.execute(updt_fecha)
    """

    updt_fecha = "Select  case when abs(producto) = 7700 then 'PP24M'  when abs(producto) = 7600 then 'PP18M'"
    updt_fecha += " when abs(producto) = 6300 then 'PP12M' when abs(producto) = 6800 then 'P_Digital' end as prod, "
    updt_fecha += " sum(monto) as vol, count(*) as regs, count(distinct num_cuenta) as ctas"
    updt_fecha += " from hiscred_pres"
    updt_fecha += " where abs(transaccion) in (7469, 7506, 7706, 7920)"
    updt_fecha += " and abs(producto) in (7700, 7600, 6300, 6800)"
    updt_fecha += " group by  case when abs(producto) = 7700 then 'PP24M'  when abs(producto) = 7600 then 'PP18M'"
    updt_fecha += " when abs(producto) = 6300 then 'PP12M' when abs(producto) = 6800 then 'P_Digital' end "
    updt_fecha += ";"
    cursorObj.execute(updt_fecha)
    
    list = cursorObj.fetchall()
    for row in list:
        print(fecha,row[0],row[1],row[2],row[3])
    
    conn.close()
    