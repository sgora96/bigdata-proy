# --- preprocess_schools.py ---
import json
import os

import pandas as pd

# Obtener el directorio donde se ejecuta el script
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


def fusionar_nombres(row):
    """
    Crea un nombre fusionado: "Establecimiento - Sede",
    limpiando redundancias de la palabra "SEDE".
    """
    establecimiento = str(row.get("nombre_establecimiento", "")).strip()
    sede = str(
        row.get("nombre_sede_original", "")
    ).strip()  # Usar el nombre original de la sede

    # Caso base: si no hay nombre de establecimiento o sede, retornar el que exista
    if not establecimiento:
        return sede
    if not sede:
        return establecimiento
    # Si ambos nombres son idénticos, retornar solo uno
    if establecimiento == sede:
        return establecimiento

    # Lógica de limpieza para la palabra "SEDE"
    sede_upper = sede.upper()

    # Si la sede ya contiene "SEDE" al inicio, no repetirlo
    if sede_upper.count("SEDE") > 0:
        nombre_final = f"{establecimiento} - {sede}"
    # Caso general: concatenar
    else:
        nombre_final = f"{establecimiento} - SEDE {sede}"

    # Limpieza final de espacios dobles
    return " ".join(nombre_final.split())


def preprocess_school_data(csv_path, json_output_path):
    """
    Lee el CSV de colegios, limpia los datos, calcula un consumo base
    y guarda la información relevante por sede en un archivo JSON.
    """
    if not os.path.exists(csv_path):
        print(f"Error: El archivo CSV no se encontró en {csv_path}")
        return None

    try:
        df = pd.read_csv(csv_path)

        # --- Limpieza y Selección de Columnas ---
        # Renombrar columnas para facilitar el acceso (opcional)
        df.rename(
            columns={
                "Id de la sede educativa": "id_sede",
                "Nombre de la sede educativa": "nombre_sede_original",
                "Nombre del establecimiento educativo": "nombre_establecimiento",
                "Nombre de la Localidad": "localidad",
                "Matriculados por sede en el año 2023": "matriculados_2023",
                "Total consumo de energía en el año 2023 (kWh)": "consumo_anual_2023_kwh",
                "coord_x": "lon",
                "coord_y": "lat",
            },
            inplace=True,
        )

        # Fusionar nombres de sedes y establecimientos
        df["nombre_sede"] = df.apply(fusionar_nombres, axis=1)

        # Seleccionar columnas relevantes
        relevant_cols = [
            "id_sede",
            "nombre_sede",
            "localidad",
            "matriculados_2023",
            "consumo_anual_2023_kwh",
            "lon",
            "lat",
        ]
        df_schools = df[relevant_cols].copy()

        # --- Limpiar Datos Numéricos ---
        # Quitar comas y convertir a número. Manejar errores (NaN si no se puede convertir)
        for col in ["matriculados_2023", "consumo_anual_2023_kwh"]:
            df_schools[col] = (
                df_schools[col].astype(str).str.replace(",", "", regex=False)
            )
            df_schools[col] = pd.to_numeric(df_schools[col], errors="coerce")

        # Asegurarse que lat/lon son strings, reemplazar coma por punto, luego convertir a float
        for col in ["lat", "lon"]:
            if col in df_schools.columns:  # Verificar que existen
                df_schools[col] = (
                    df_schools[col].astype(str).str.replace(",", ".", regex=False)
                )
                df_schools[col] = pd.to_numeric(
                    df_schools[col], errors="coerce"
                )  # Convertir a float

        
        df_schools.fillna(0, inplace=True)
        
        # Filtrar registros que no reporten consumo normal
        # Tiene que ser de al menos 500 kWh al año
        df_schools = df_schools[df_schools['consumo_anual_2023_kwh'] > 500].copy()

        # Si después de filtrar no queda ningún registro, informar y salir
        if df_schools.empty:
             print("No quedaron registros después de filtrar por consumo cero. No se generará el archivo JSON.")
             return None
        

        # --- Eliminar Duplicados (asegurar una entrada por sede) ---
        df_schools.drop_duplicates(subset=["id_sede"], keep="first", inplace=True)

        # --- Calcular Base de Consumo Horario ---
        # Consumo anual / horas en un año (aprox)
        # Si consumo anual es 0, la base será 0
        df_schools["base_consumo_hora_2023_kwh"] = df_schools[
            "consumo_anual_2023_kwh"
        ] / (365 * 24)

        # --- Preparar para JSON ---
        # Convertir a lista de diccionarios
        schools_config = df_schools.to_dict(orient="records")

        # Guardar en archivo JSON
        with open(json_output_path, "w", encoding="utf-8") as f:
            json.dump(schools_config, f, ensure_ascii=False, indent=4)

        print(
            f"Preprocesamiento completado. {len(schools_config)} sedes únicas guardadas en {json_output_path}"
        )
        return schools_config

    except Exception as e:
        print(f"Error durante el preprocesamiento: {e}")
        return None


if __name__ == "__main__":
    CSV_FILE_PATH = __location__ + "/export.csv"
    JSON_CONFIG_PATH = __location__ + "/schools_config.json"
    preprocess_school_data(CSV_FILE_PATH, JSON_CONFIG_PATH)
