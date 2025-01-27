# Función para la conexión a MySQL.

import mysql.connector

def create_connection():
    return mysql.connector.connect(
        host="localhost",        
        user="root",       
        password="MariaPaula18*", 
        database="jikkosoft" 
    )