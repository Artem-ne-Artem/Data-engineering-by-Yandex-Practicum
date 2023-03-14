import vertica_python

conn_info = {'host': '51.250.75.20',
             'port': '5433',
             'user': 'DDDZ2000YANDEXRU',
             'password': 'sIjgINZywlB6O1f',
             'database': 'dwh',
             # Вначале автокоммит понадобится, а позже решите сами.
             'autocommit': True
             }

def try_select(conn_info=conn_info):
    # Рекомендуем использовать соединение:
    with vertica_python.connect(**conn_info) as conn:
        # Напишите здесь курсор, который выполняет запрос SELECT 1 as a1;
        cur = conn.cursor()
        cur.execute("""SELECT 1 as a1""")
        res = cur.fetchall()
        return res