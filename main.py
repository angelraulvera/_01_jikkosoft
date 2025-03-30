# Archivo principal que coordina todo.

import pandas as pd
from database import create_connection
from process_txt import read_txt_in_batches, fill_missing_values
from process_csv import read_csv_maximos, read_csv_minimos, read_csv_tarifas
from utils import get_statistics

# Variables acumuladoras para el seguimiento
total_rows = 0
total_tasa_calculada = 0.0


def process_and_insert_batches(folder_path, tarifas, minimos, maximos, connection):
    global total_rows, total_tasa_calculada  # Variables acumuladoras globales
    cursor = connection.cursor()

    query_insert = """
        INSERT INTO consumos (id, year, destino, estrato, consumo, tasa_calculada, minimo, maximo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for batch in read_txt_in_batches(folder_path):
        # Fusionar con tarifas para intentar obtener los valores
        batch = pd.merge(batch, tarifas, on=["destino", "estrato"], how="left")

        # Rellenar valores nulos en estrato y tarifa mas detalle en el archivo process_txt.py
        batch = fill_missing_values(batch)

        # Fusiones y cálculos maximos y minimos
        batch["tasa_calculada"] = batch["tarifa"] * batch["consumo"]
        batch = pd.merge(batch, minimos, on="year", how="left")
        batch = pd.merge(batch, maximos, on=["year", "destino"], how="left")

        # Ajustar tasa_calculada según mínimos y máximos
        batch["tasa_calculada"] = batch["tasa_calculada"].clip(batch["minimo"], batch["maximo"])
        
        # Reemplazar NaN en la columna "maximo" solo para destinos que no sean Comercial o Industrial
        batch.loc[~batch["destino"].isin(["Comercial", "Industrial"]), "maximo"] = batch["maximo"].fillna(0.0)

        # Reemplazar NaN por None para todos los valores faltantes
        batch = batch.where(pd.notnull(batch), None)
        
        # Actualizar estadísticas dinámicas luego de cadata microbatch de 500 filas
        total_rows += len(batch) 
        total_tasa_calculada += batch["tasa_calculada"].sum()  
        
        # Guardar estadísticas en un archivo de texto
        with open("estadisticas.txt", "a") as file:
            file.write(f"Filas cargadas: {total_rows}, "
                       f"Sumatoria de tasa calculada: {total_tasa_calculada:.2f}\n")

        # Validación de datos antes de insertar en la base de datos
        #print("Muestra del batch antes de la inserción:")
        #print(batch.head())

        # Insertar en la base de datos
        for row in batch.itertuples(index=False):
            cursor.execute(query_insert, (row.id, row.year, row.destino, row.estrato, row.consumo, row.tasa_calculada, row.minimo, row.maximo))
        connection.commit()
        
        # Imprimir seguimiento del progreso
        print(f"Filas cargadas hasta ahora: {total_rows}")
        print(f"Sumatoria de tasa calculada hasta ahora: {total_tasa_calculada:.2f}")

# Cambia la ruta para aplicarlo en tu entorno
if __name__ == "__main__":
    folder_path = r"C:/Users/angel/OneDrive/Documentos/_01_Portafolio/_01_Jikkosoft/02_dataset"
    maximos_path = r"C:/Users/angel/OneDrive/Documentos/_01_Portafolio/_01_Jikkosoft/maximos.csv"
    minimos_path = r"C:/Users/angel/OneDrive/Documentos/_01_Portafolio/_01_Jikkosoft/minimos.csv"
    tarifas_path = r"C:/Users/angel/OneDrive/Documentos/_01_Portafolio/_01_Jikkosoft/tarifa_por_destino.csv"

    connection = create_connection()

    try:
        maximos = read_csv_maximos(maximos_path)
        minimos = read_csv_minimos(minimos_path)
        tarifas = read_csv_tarifas(tarifas_path)

        #validación del ajuste en los nombres de las columnas
        #print("Columnas de tarifas:", tarifas.columns)
        #print("Columnas de maximos:", maximos.columns)
        #print("Columnas de minimos:", minimos.columns)

        process_and_insert_batches(folder_path, tarifas, minimos, maximos, connection)
        
        get_statistics(connection)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        connection.close()