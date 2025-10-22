# generate_sedes_sql.py
import json
import os
from datetime import datetime, timezone

# Configuración
JSON_CONFIG_PATH = 'schools_config.json'
OUTPUT_SQL_FILE = '01_insert_sedes.sql'
SEDES_TABLE_NAME = 'sedes' # Nombre de tu tabla de sedes

def format_sql_value(value):
    """Formatea valores para SQL, manejando None y strings."""
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        # Escapar comillas simples en strings
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"

def generate_sql():
    # Cargar configuración
    if not os.path.exists(JSON_CONFIG_PATH):
        print(f"Error: Archivo de configuración '{JSON_CONFIG_PATH}' no encontrado.")
        return
    try:
        with open(JSON_CONFIG_PATH, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        if not config_data:
            print(f"Error: El archivo de configuración está vacío o no es válido.")
            return
    except Exception as e:
        print(f"Error cargando el archivo de configuración: {e}")
        return

    # Abrir archivo SQL de salida
    try:
        with open(OUTPUT_SQL_FILE, 'w', encoding='utf-8') as f:
            f.write(f"-- Script para insertar sedes desde {JSON_CONFIG_PATH}\n")
            f.write(f"-- Generado en: {datetime.now(timezone.utc).isoformat()}\n\n")

            # Podríamos hacer un solo INSERT con múltiples VALUES, pero
            # para las sedes (pocas filas) INSERTs individuales son más legibles
            # y manejan mejor conflictos si se ejecuta varias veces (con ON CONFLICT)
            f.write(f"INSERT INTO {SEDES_TABLE_NAME} (id, nombre_sede, id_localidad, lat, lon, fecha_creacion_utc, fecha_actualizacion_utc)\nVALUES\n")

            values_lines = []
            now_ts = datetime.now(timezone.utc).isoformat()

            for i, school in enumerate(config_data):
                sede_id = school.get('id_sede')
                nombre = school.get('nombre_sede')
                localidad = school.get('localidad')
                lat = school.get('lat')
                lon = school.get('lon')

                # Validar que tenemos al menos el ID
                if sede_id is None:
                    print(f"Advertencia: Registro omitido por falta de id_sede: {school}")
                    continue

                values_line = (
                    f"  ({format_sql_value(sede_id)}, "
                    f"{format_sql_value(nombre)}, "
                    f"{format_sql_value(localidad)}, "
                    f"{format_sql_value(lat)}, "
                    f"{format_sql_value(lon)}, "
                    f"'{now_ts}', '{now_ts}')" # Usar el mismo timestamp para creación/actualización inicial
                )
                values_lines.append(values_line)

            # Unir las líneas con comas y añadir punto y coma al final
            f.write(",\n".join(values_lines))

            # Añadir cláusula ON CONFLICT (para PostgreSQL) para evitar errores si ya existe
            # Esto actualiza la fila si el ID ya existe (Upsert)
            f.write(f"\nON CONFLICT (id) DO UPDATE SET\n"
                    f"  nombre_sede = EXCLUDED.nombre_sede,\n"
                    f"  id_localidad = EXCLUDED.id_localidad,\n"
                    f"  lat = EXCLUDED.lat,\n"
                    f"  lon = EXCLUDED.lon,\n"
                    f"  fecha_actualizacion_utc = EXCLUDED.fecha_actualizacion_utc;\n")

            print(f"Archivo SQL '{OUTPUT_SQL_FILE}' generado con {len(values_lines)} sedes.")

    except Exception as e:
        print(f"Error generando el archivo SQL de sedes: {e}")

if __name__ == "__main__":
    generate_sql()