import mysql.connector


config = {
  'user': 'swx_python_app',
  'password': '***',
  'host': '174.129.58.221',
  'database': 'swx_db',
  'raise_on_warnings': True,
}


def connect():
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        print(err)
        exit(1)

    return cnx
