# Funciones para manejar los archivos .txt.

import pandas as pd
import os

def read_txt_in_batches(folder_path, batch_size=500):
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".txt"):
            file_path = os.path.join(folder_path, file_name)
            for chunk in pd.read_csv(file_path, sep=",", chunksize=batch_size):
                chunk.rename(columns={"año": "year", "Destino": "destino"}, inplace=True)
                yield chunk       
                
def fill_missing_values(batch):
    # Rellenar valores faltantes de estrato según destino
    batch.loc[(batch["destino"].isin(["Comercial", "Industrial", "Especial", "Otros"])) & (batch["estrato"].isnull()), "estrato"] = 7
    batch.loc[(batch["destino"] == "Oficial") & (batch["estrato"].isnull()), "estrato"] = 4

    # Rellenar valores faltantes de tarifa según destino y estrato
    batch.loc[(batch["destino"].isin(["Comercial", "Industrial", "Especial", "Otros"])) & (batch["tarifa"].isnull()), "tarifa"] = 0.017
    batch.loc[(batch["destino"] == "Oficial") & (batch["tarifa"].isnull()), "tarifa"] = 0.01
       
    return batch

  
          