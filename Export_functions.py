# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 08:26:11 2021

@author: Francisco Espinosa
"""
import pandas as pd
import datetime
import os

#constants
recs_limit = 20000

def exporta_archivos_ready(format, template, path, df):
    if format != 'sms' and format != 'email':
        print('Necesitas proprocionar un tipo de archivo correcto (sms/email).')
        return
    if template == '' or template is None:
        print('Necesitas proprocionar un nombre de plantilla.')
        return
    if len(path)<3 or path == '':
        print('Necesitas proprocionar una ruta valida.')
        return
    
    if format.upper() == 'SMS':
        Header = "BANCOPPEL|productos||sms3|" + template + "|"
    elif format.upper() == 'EMAIL':
        Header = "BANCOPPEL|productos||mail1|" + template + "|"

    
    if isinstance(df,pd.DataFrame):
        NameFile = format.upper() + "_" + template + "_" + datetime.datetime.now().strftime("%b%y")
        Statfile = "Resumen_sesion_" + template + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
        total_rows = df.shape[0]
        ToGo = total_rows
        customer_id = ''
        row_count = 0
        pkgs = 0
        for index, Row in df.iterrows():
            if row_count == 0:
                FNameFile = NameFile + ('-0' if pkgs <= 9 else '-') + str(pkgs) + ".ready"
                f = open(path + '\\' + FNameFile, "a")
                f.write(Header + str(recs_limit if ToGo > recs_limit else ToGo))
                
            if row_count == (recs_limit-1):
                f.write(str(row_count) + '|' + str(Row[1]) + '||||||' + template + '|nombre=' + str(Row[2]) + chr(10))
                row_count = 0
                pkgs +=1
                s = open(path + '\\' + Statfile, "a")
                s.write(FNameFile + "(" + str(recs_limit if ToGo > recs_limit else ToGo) + " regs)"
                s.close()
                ToGo -= recs_limit
                f.write('<EOF>')
                f.close()
            else:
                row_count +=1
                f.write(str(row_count) + '|' + str(Row[1]) + '||||||' + template + '|nombre=' + str(Row[2]) + chr(10))

        f.write('<EOF>')
        f.close()
        s = open(path + '\\' + Statfile, "a")
        s.write(FNameFile + "(" + str(recs_limit if ToGo > recs_limit else ToGo) + " regs)"
        s.close()
    else:
        print('Necesitas proprocionar un dataframe válido.')
        return