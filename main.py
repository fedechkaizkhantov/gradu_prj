import sqlite3
import pandas as pd
import os
import openpyxl
# import jaydebeapi
from ddl import *
from datetime import datetime
import shutil
from os import path

# import jaydebeapi
# connect = jaydebeapi.connect(
# 	'oracle.jdbc.driver.OracleDriver',
# 	'jdbc:oracle:de1h/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
# 	['de1h', 'bilbobaggins'],
# 	'ojdbc7.jar') 
# cursor = connect.cursor()

connect = sqlite3.connect('PROJECT.db')
cursor = connect.cursor()

#читаем файлы транзакций, переименовываем и переносим в папку archive
def csv2sql(path_to_file):
	df = pd.read_csv(path_to_file, sep=';')
	cursor = connect.cursor()
	cursor.executemany('''INSERT INTO S_31_DWH_STG_transactions (
		transaction_id,
		transaction_date,
		amount,
		card_num,
		oper_type,
		oper_result,
		terminal
		) VALUES (?,?,?,?,?,?,?)''',df.values.tolist())
	connect.commit()
	os.rename(path_to_file, path_to_file + ".backup")
	destination_path = "archive"
	shutil.move(path_to_file + ".backup", destination_path)



# читаем файлы блэклист паспортов(xlsx)

def xlsx2sql(path_to_file):
	dateparse = lambda x: datetime.strftime(x, '%Y-%m-%d')
	data_df = pd.read_excel(path_to_file, header = 0, usecols = None, parse_dates=['date'], date_parser=dateparse)
	cursor = connect.cursor()
	cursor.executemany('''INSERT INTO S_31_DWH_STG_pssprt_blcklst (
		entry_dt,
		passport_num
		) VALUES (?,?)''',data_df.values.tolist())
	connect.commit()
	os.rename(path_to_file, path_to_file + ".backup")
	destination_path = "archive"
	shutil.move(path_to_file + ".backup", destination_path)

# читаем файлы терминалов, переименовываем и переносим в папку 

def xlsx2sqlT(path_to_file):
	data_tr = pd.read_excel(path_to_file, header = 0, usecols = None)
	# print(data_df.values.tolist())
	cursor = connect.cursor()
	cursor.executemany('''INSERT INTO S_31_DWH_STG_terminals(
		terminal_id,
		terminal_type,
		terminal_city,
		terminal_address
		) VALUES (?,?,?,?)''',data_tr.values.tolist())
	connect.commit()
	os.rename(path_to_file, path_to_file + ".backup")
	destination_path = "archive"
	shutil.move(path_to_file + ".backup", destination_path)


# удаляем таблицы
def dropTablesBegin():
	cursor = connect.cursor()
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_STG_transactions
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_STG_pssprt_blcklst
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_STG_terminals
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_FACT_transactions
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_FACT_pssprt_blcklst
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_DIM_terminals_HIST
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_new_rows_transactions_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_new_rows_terminals_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_new_rows_passport_blcklst_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_deleted_rows_transactions_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_deleted_rows_pssprt_blcklst_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_changed_rows_trnsctns_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_changed_rows_pssprt_blcklst_tmp
		''')
	cursor.execute('''
		DROP TABLE IF EXISTS S_31_DWH_changed_rows_terminals_tmp
		''')


def showTable(tableName):
	print('_-'*20)
	print(tableName)

	print('_-'*20)
	cursor.execute(f'SELECT * FROM {tableName}')
	for row in cursor.fetchall():
		print(row)

	print('_-'*20+'\n')


def createSTG(datefile):
	source_path_tr = "transactions_"
	source_path_bl = "passport_blacklist_"
	source_path_term = "terminals_"
	if path.exists(source_path_tr + datefile + ".txt"):
		csv2sql(source_path_tr + datefile + ".txt")
	else:
		print("Файл не существует.")
	if path.exists(source_path_bl + datefile + ".xlsx"):
		xlsx2sql(source_path_bl + datefile + ".xlsx")
	else:
		print("Файл не существует.")
	if path.exists(source_path_term + datefile + ".xlsx"):
		xlsx2sqlT(source_path_term + datefile + ".xlsx")
	else:
		print("Файл не существует.")

def createtables(datefile):
	
	initTablesSTG(connect)
	createSTG(datefile)
	newRows(connect)
	deletedRows(connect)
	changedRows(connect)
	changeTables(connect)
	changerowterminal(connect)
	clearSTG(connect)
	clearTMP(connect)
# удалим все таблицы
dropTablesBegin()
# создаем счетчики
# create_sequence()
# создаем стейжинговые таблицы
initTablesSTG(connect)
# создаем таблицы хранения фактов и измерений
initTablesFACT(connect)
# создаем представления
initView(connect)
# запускаем процедуру ежедневного формирования таблиц
createtables("01032021")
createtables("02032021")
createtables("03032021")



