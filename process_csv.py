# Funciones para manejar los archivos .csv.

import pandas as pd

def read_csv_maximos(file_path):
    maximos = pd.read_csv(file_path, sep=",", decimal=",", thousands=".")
    maximos.rename(columns={"Año": "year", "Destino": "destino", "Máximo": "maximo"}, inplace=True)
    return maximos

def read_csv_minimos(file_path):
    minimos = pd.read_csv(file_path, sep=",", decimal=",", thousands=".")
    minimos.rename(columns={"Año": "year", "Mínimo": "minimo"}, inplace=True)
    return minimos

def read_csv_tarifas(file_path):
    tarifas = pd.read_csv(file_path, sep=",", decimal=",")
    tarifas["Tarifa sobre consumo"] = (
        tarifas["Tarifa sobre consumo"]
        .str.replace("%", "")
        .str.replace(",", ".")
        .astype(float) / 100
    )
    tarifas.rename(columns={"Tarifa sobre consumo": "tarifa", "Destino": "destino", "Estrato":"estrato"}, inplace=True)
    return tarifas
