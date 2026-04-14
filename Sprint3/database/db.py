from flask_mysqldb import MySQL

import mysql.connector
mysql = MySQL()
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="pharmacy_db"
    )
