# Funciones auxiliares para cálculos y estadísticas.

import mysql.connector

def get_statistics(connection):
    cursor = connection.cursor()
    query = """
        SELECT COUNT(*) AS total_rows,
               AVG(tasa_calculada) AS avg_tasa,
               SUM(tasa_calculada) AS sum_tasa,
               MIN(tasa_calculada) AS min_tasa,
               MAX(tasa_calculada) AS max_tasa
        FROM consumos;
    """
    cursor.execute(query)
    result = cursor.fetchone()
        
    # Convertir y formatear los resultados
    total_rows = result[0]
    avg_tasa = float(result[1]) if result[1] is not None else None
    sum_tasa = float(result[2]) if result[2] is not None else None
    min_tasa = float(result[3]) if result[3] is not None else None
    max_tasa = float(result[4]) if result[4] is not None else None

    print(f"Estadísticas finales:")
    print(f"Total filas: {total_rows}")
    print(f"Promedio de tasa calculada: {avg_tasa:.2f}")
    print(f"Sumatoria de tasa calculada: {sum_tasa:.2f}")
    print(f"Tasa mínima: {min_tasa:.2f}")
    print(f"Tasa máxima: {max_tasa:.2f}")
    cursor.close()
