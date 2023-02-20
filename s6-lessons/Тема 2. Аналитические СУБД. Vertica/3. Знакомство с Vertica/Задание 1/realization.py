import vertica_python

conn_info = {'host': '# Адрес сервера из инструкции', 
             'port': '# Порт из инструкции',
             'user': '# Полученный логин',       
             'password': '# Пароль',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

def try_select(conn_info=conn_info):
	# И рекомендуем использовать соединение вот так
	with vertica_python.connect(**conn_info) as conn:
		# Select 1 — ваш код здесь; 

		res = cur.fetchall()
		return res