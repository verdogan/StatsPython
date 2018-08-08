import mysql.connector
from pytz import timezone
from datetime import datetime


config = {
  'user': 'swx_python_app',
  'password': 'P5drKMrmV@HK4bmG',
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


def currtime():
    tz = timezone('EST')
    now_est = datetime.now(tz)
    now = now_est.strftime("%Y-%m-%d %H:%M:%S")

    return now