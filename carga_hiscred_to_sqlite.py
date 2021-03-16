# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 23:30:09 2019

@author: fespinosa
"""

import sqlite3
import pandas
import os
import Complement_functions as cf

database_path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\DWH\\'
database_filename = 'trxn_diarias.db'
conn = sqlite3.connect(database_path + database_filename)

filename_path = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\tmp\\Carga\\'
filename_path_bck = 'C:\\Users\\fespinosa\\Documents\\Trabajo\\tmp\\Carga\\historico\\'

tables = ['hiscred_pres_flex','hiscred_ADN','Hiscred_GC','Hiscred_Oro','hiscred_pres','hiscred']
filesnames = ['his_credito_pres_flex','hisantic_nom','Hiscred_GrupoCoppel','Hiscred_Oro','his_credito_pres','his_credito']
extensions = ['.txt','.txt','.txt','.txt','','']

for step in range(6):
    filepattern = filesnames[step]
    tablename = tables[step]
    files = []
    #print(step, filepattern, tablename)
    files = cf.listado_archivos(filename_path, filepattern)
    
    if tablename =='Hiscred_GC' or tablename == 'Hiscred_Oro':
        colnames = [
        'PRODUCTO',
        'NUM_CUENTA',
        'NUM_CLIENTE',
        'SUCURSAL',
        'FOLIO_SUC',
        'NUM_TDC',
        'MONTO',
        'DESCRIPCION',
        'CODIGO_FUN',
        'CODIGO_REF',
        'TRANSACCION',
        'CONTABLE',
        'SECUENCIA',
        'CTA_CARGO',
        'CTA_ABONO',
        'FECHA_MOV',
        'field17',
        'Field18',
        'Field19',
        'Field20',
        'Field21',
        'control',
        'control2']
    elif tablename =='hiscred_ADN':
        colnames = [
        'PRODUCTO',
        'NUM_CUENTA',
        'NUM_CLIENTE',
        'SUCURSAL',
        'FOLIO_SUC',
        'NUM_TDC',
        'MONTO',
        'DESCRIPCION',
        'CODIGO_FUN',
        'CODIGO_REF',
        'TRANSACCION',
        'CONTABLE',
        'SECUENCIA',
        'CTA_CARGO',
        'CTA_ABONO',
        'FECHA_MOV',
        'Control']
    elif tablename =='hiscred':
        colnames = [
        'PRODUCTO',
        'NUM_CUENTA',
        'NUM_CLIENTE',
        'SUCURSAL',
        'FOLIO_SUC',
        'NUM_TDC',
        'MONTO',
        'DESCRIPCION',
        'CODIGO_FUN',
        'CODIGO_REF',
        'TRANSACCION',
        'CONTABLE',
        'SECUENCIA',
        'CTA_CARGO',
        'CTA_ABONO',
        'FECHA_MOV',
        'id_terminal',
        'referencia',
        'fecha',
        'secuencia2',
        'secuencia_origen',
        'control',
        'control2']
    else:
        colnames = [
        'PRODUCTO',
        'NUM_CUENTA',
        'NUM_CLIENTE',
        'SUCURSAL',
        'FOLIO_SUC',
        'NUM_TDC',
        'MONTO',
        'DESCRIPCION',
        'CODIGO_FUN',
        'CODIGO_REF',
        'TRANSACCION',
        'CONTABLE',
        'SECUENCIA',
        'CTA_CARGO',
        'CTA_ABONO',
        'FECHA_MOV',
        'Control',
        'Control2']

    for filename in files:
        csvfile = filename_path  + filename
        bckfile = filename_path_bck  + filename
        try:
            #print(step, csvfile)
            df = pandas.read_table(csvfile, encoding='latin-1', header = None, names=colnames, sep='|', dtype = str)
            df.to_sql(tablename, conn, if_exists='append', index=False)
            cursorObj = conn.cursor()
            updt_fecha = 'update ' + tablename + " set fecha_mov = substr(fecha_mov,7,4) || '-' || substr(fecha_mov,1,2) || '-' || substr(fecha_mov,4,2),"
            if tablename == 'hiscred_pres_flex' or  tablename == 'hiscred_pres':
                updt_fecha += " Control2 = 'ok'" 
                updt_fecha += " where (Control2 is null or Control2 ='');"
            else:
                updt_fecha += " control = 'ok'" 
                updt_fecha += " where (control is null or control ='');"
            cursorObj.execute(updt_fecha)
            #limpia_rep = 'delete from ' + tablename
            #limpia_rep += "  where secuencia in ('2', '3');" 
            #cursorObj.execute(limpia_rep)
            conn.commit()
            cursorObj.close()
            os.replace(csvfile, bckfile)
            
        except Exception as e:
                print('Error: {}'.format(str(e)) + ' Archivo:' + str(filename))    

conn.close()