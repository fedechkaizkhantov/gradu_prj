def initTablesSTG(connect):
	cursor = connect.cursor()
# создаем таблицы
	cursor.execute(''' 
    	CREATE TABLE if not exists S_31_DWH_STG_transactions(
			transaction_id varchar(128),
			transaction_date datetime,
			amount decimal(10,2),
			card_num varchar(128),
			oper_type varchar(128),
			oper_result varchar(128),
			terminal varchar(128)
		)
		''')
	cursor.execute(''' 
    	CREATE TABLE if not exists S_31_DWH_STG_pssprt_blcklst(
			entry_dt date,
			passport_num varchar(128)
		)
		''')


	cursor.execute(''' 
    	CREATE TABLE if not exists S_31_DWH_STG_terminals(
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128)
		)
		''')
# создаем таблицы измерений и фактов
def initTablesFACT(connect):
	cursor = connect.cursor()
	cursor.execute(''' 
    	CREATE TABLE if not exists S_31_DWH_FACT_transactions(
			id integer primary key autoincrement,
			transaction_id varchar(128),
			transaction_date datetime,
			amount decimal(10,2),
			card_num varchar(128),
			oper_type varchar(128),
			oper_result varchar(128),
			terminal varchar(128),
			effective_from datetime default current_timestamp,
			effective_to datetime default (datetime('2999-12-31 23:59:59')),
			deleted_flg integer check(deleted_flg in (0, 1)) default 0
		)
		''')
	cursor.execute(''' 
    	CREATE TABLE if not exists S_31_DWH_FACT_pssprt_blcklst(
    		id integer primary key autoincrement,
			entry_dt date,
			passport_num varchar(128),
			effective_from datetime default current_timestamp,
			effective_to datetime default (datetime('2999-12-31 23:59:59')),
			deleted_flg integer check(deleted_flg in (0, 1)) default 0
		)
		''')
	cursor.execute(''' 
    	CREATE TABLE if not exists S_31_DWH_DIM_terminals_HIST(
			id integer primary key autoincrement,	
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128),
			create_dt datetime default current_timestamp,
			update_dt datetime default current_timestamp
		)
		''')
# создаем представления
def initView(connect):
	cursor = connect.cursor()
	cursor.execute('''
		CREATE view if not exists S_31_v_transactions as 
		select
			transaction_id,
			transaction_date,
			amount,
			card_num,
			oper_type,
			oper_result,
			terminal
		from S_31_DWH_FACT_transactions
		where current_timestamp between effective_from and effective_to
		and deleted_flg = 0
	''')

	cursor.execute('''
		CREATE view if not exists S_31_v_pssprt_blcklst as 
		select
			entry_dt,
			passport_num
		from S_31_DWH_FACT_pssprt_blcklst
		where deleted_flg = 0
	''')
	

# новые строки таблиц
def newRows(connect):

	cursor = connect.cursor()
	cursor.execute('''
		CREATE TABLE if not exists S_31_DWH_new_rows_transactions_tmp as
			SELECT			
				t1.transaction_id,
				t1.transaction_date,
				t1.amount,
				t1.card_num,
				t1.oper_type,
				t1.oper_result,
				t1.terminal
			FROM S_31_DWH_STG_transactions t1
			LEFT JOIN S_31_v_transactions t2
			ON t1.transaction_id = t2.transaction_id
			WHERE t2.transaction_id IS null
		''')
	cursor.execute('''
		CREATE TABLE if not exists S_31_DWH_new_rows_terminals_tmp as
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address						
			FROM S_31_DWH_STG_terminals t1
			LEFT JOIN S_31_DWH_DIM_terminals_HIST t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t2.terminal_id IS null
		''')
	cursor.execute('''
		CREATE TABLE S_31_DWH_new_rows_passport_blcklst_tmp as
			SELECT
				t1.entry_dt,			
				t1.passport_num
			FROM S_31_DWH_STG_pssprt_blcklst t1
			LEFT JOIN S_31_v_pssprt_blcklst t2
			ON t1.passport_num = t2.passport_num
			WHERE t2.passport_num IS null
		''')

def deletedRows(connect):
	cursor = connect.cursor()
	
	cursor.execute('''
		CREATE TABLE if not exists S_31_DWH_deleted_rows_pssprt_blcklst_tmp as
			SELECT
				t1.entry_dt,	
				t1.passport_num
			FROM S_31_v_pssprt_blcklst t1
			LEFT JOIN S_31_DWH_STG_pssprt_blcklst t2
			ON t1.passport_num = t2.passport_num
			WHERE t2.passport_num IS null
		''')


def changedRows(connect):
	cursor = connect.cursor()
	cursor.execute('''
		CREATE TABLE if not exists S_31_DWH_changed_rows_trnsctns_tmp as
			SELECT	
				t1.transaction_id,
				t1.transaction_date,
				t1.amount,
				t1.card_num,
				t1.oper_type,
				t1.oper_result,
				t1.terminal		
			FROM S_31_DWH_STG_transactions t1
			INNER JOIN S_31_v_transactions t2
			ON t1.transaction_id = t2.transaction_id
			AND(t1.transaction_date <> t2.transaction_date
				OR t1.amount <> t2.amount
				OR t1.card_num <> t2.card_num
				OR t1.oper_type <> t2.oper_type
				OR t1.oper_result <> t2.oper_result
				OR t1.terminal <> t2.terminal)
		''')
	cursor.execute('''
		CREATE TABLE if not exists S_31_DWH_changed_rows_pssprt_blcklst_tmp as
			SELECT
				t1.entry_dt,	
				t1.passport_num
			FROM S_31_DWH_STG_pssprt_blcklst t1
			INNER JOIN S_31_v_pssprt_blcklst t2
			ON t1.passport_num = t2.passport_num
			AND t1.entry_dt <> t2.entry_dt
		''')
	cursor.execute('''
		CREATE TABLE if not exists S_31_DWH_changed_rows_terminals_tmp as
			SELECT
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address						
			FROM S_31_DWH_STG_terminals t1
			INNER JOIN S_31_DWH_DIM_terminals_HIST t2
			ON t1.terminal_id = t2.terminal_id
			AND(t1.terminal_type <> t2.terminal_type
				OR t1.terminal_city <> t2.terminal_city
				OR t1.terminal_address <> t2.terminal_address)
		''')
def changeTables(connect):
	cursor = connect.cursor()
	cursor.execute(''' 
		UPDATE S_31_DWH_FACT_transactions
		set effective_to = datetime('now', '-1 second')
		where transaction_id in (select transaction_id from S_31_DWH_changed_rows_trnsctns_tmp)
		and effective_to = datetime('2999-12-31 23:59:59')
	''')
	cursor.execute(''' 
		UPDATE S_31_DWH_FACT_pssprt_blcklst
		set effective_to = datetime('now', '-1 second')
		where passport_num in (select passport_num from S_31_DWH_changed_rows_pssprt_blcklst_tmp)
		and effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute(''' 
		UPDATE S_31_DWH_FACT_pssprt_blcklst
		set effective_to = datetime('now', '-1 second')
		where passport_num in (select passport_num from S_31_DWH_deleted_rows_pssprt_blcklst_tmp)
		and effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute('''
		INSERT INTO S_31_DWH_FACT_transactions (transaction_id,transaction_date,amount,card_num,oper_type,oper_result,terminal)
		SELECT transaction_id,transaction_date,amount,card_num,oper_type,oper_result,terminal
		FROM S_31_DWH_new_rows_transactions_tmp
		''')
	cursor.execute('''
		INSERT INTO S_31_DWH_FACT_transactions (transaction_id,transaction_date,amount,card_num,oper_type,oper_result,terminal)
		SELECT transaction_id,transaction_date,amount,card_num,oper_type,oper_result,terminal
		FROM S_31_DWH_changed_rows_trnsctns_tmp
		''')
	
	cursor.execute('''
		INSERT INTO S_31_DWH_FACT_pssprt_blcklst (entry_dt,passport_num)
		SELECT entry_dt,passport_num
		FROM S_31_DWH_new_rows_passport_blcklst_tmp
		''')
	cursor.execute('''
		INSERT INTO S_31_DWH_FACT_pssprt_blcklst (entry_dt,passport_num)
		SELECT entry_dt,passport_num
		FROM S_31_DWH_changed_rows_pssprt_blcklst_tmp
		''')
	cursor.execute('''
		INSERT INTO S_31_DWH_FACT_pssprt_blcklst (entry_dt,passport_num,deleted_flg)
		SELECT entry_dt,passport_num, 1
		FROM S_31_DWH_deleted_rows_pssprt_blcklst_tmp
		''')
	cursor.execute('''
		INSERT INTO S_31_DWH_DIM_terminals_HIST (terminal_id,terminal_type,terminal_city,terminal_address)
		SELECT terminal_id,terminal_type,terminal_city,terminal_address
		FROM S_31_DWH_new_rows_terminals_tmp		
		''')
	connect.commit()
def changerowterminal(connect):
	cursor = connect.cursor()
	cursor.execute('select * from S_31_DWH_changed_rows_terminals_tmp')
	next_row = cursor.fetchall()
	for row in next_row:
		cursor.execute('''
				UPDATE S_31_DWH_DIM_terminals_HIST
				SET 
					terminal_type = ?,
					terminal_city = ?,
					terminal_address = ?,
					update_dt = datetime ('now')
				WHERE terminal_id = ?''',[row[1],row[2],row[3],row[0]])
		
	connect.commit()

def clearSTG(connect):
	cursor = connect.cursor()
	try:
		cursor = connect.cursor()
		cursor.execute('''
		DROP TABLE S_31_DWH_STG_transactions
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_STG_pssprt_blcklst
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_STG_terminals
		''')
		connect.commit()
	except:
		print("Ошибка при работе с SQLite")

def clearTMP(connect):
	cursor = connect.cursor()
	try:
		cursor = connect.cursor()
		cursor.execute('''
		DROP TABLE S_31_DWH_new_rows_passport_blcklst_tmp
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_changed_rows_pssprt_blcklst_tmp
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_deleted_rows_pssprt_blcklst_tmp
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_new_rows_transactions_tmp
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_changed_rows_trnsctns_tmp
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_new_rows_terminals_tmp
		''')
		cursor.execute('''
		DROP TABLE S_31_DWH_changed_rows_terminals_tmp
		''')


		connect.commit()
	except:
		print("Ошибка при работе с SQLite")
