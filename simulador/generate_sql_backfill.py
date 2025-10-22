# generate_sql_backfill.py
import argparse
import json
import os
import random  # Lo mantenemos para la lógica de factores, pero usaremos promedios
import time
from datetime import datetime, timedelta, timezone

# Reutilizar lógica de simulación (pero la haremos determinista)
from simulador_energia import (
    get_factores_simu,  # Asume que simulador_energia.py está accesible
)

# --- Configuración ---
JSON_CONFIG_PATH = "schools_config.json"
LECTURAS_TABLE_NAME = "lecturas"
DEFAULT_OUTPUT_SQL_FILE = "02_insert_lecturas_backfill.sql"
INSERT_BATCH_SIZE = 1000  # Número de filas por cada sentencia INSERT


def format_sql_value(value):
    """Formatea valores para SQL, manejando None y strings."""
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"  # Para la columna 'procesado'
    elif isinstance(value, datetime):
        # Formato ISO 8601 es generalmente bien aceptado por PostgreSQL
        return f"'{value.isoformat()}'"
    else:
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"


def simular_consumo_determinista(school_info, timestamp, intervalo_minutos):
    """
    Simula el consumo de forma determinista usando promedios de factores.
    """
    base_hourly_kwh = school_info.get("base_consumo_hora_2023_kwh", 0)
    base_hourly_kwh = school_info.get('base_consumo_hora_2023_kwh', 0)

    # Escalar la base al intervalo deseado (si la base es horaria)
    base_interval_kwh = base_hourly_kwh * (intervalo_minutos / 60.0)

    if base_interval_kwh <= 0:
        return 0 # Si la base es cero, el consumo simulado es cero

    factors = get_factores_simu(timestamp)

    # Aplicar factores multiplicativos
    simulated_kwh = (base_interval_kwh
                     * factors['time']
                     * factors['day']
                     * factors['month']
                     * factors['trend'])

    # Añadir ruido aleatorio para variabilidad
    simulated_kwh *= random.uniform(0.85, 1.15)

    # Asegurar que el consumo no sea negativo
    return max(0, round(simulated_kwh, 3)) # Redondear a 3 decimales


def generate_sql_backfill(
    config_data, start_date, end_date, interval_minutes, output_file
):
    """Genera el archivo SQL con INSERTs para las lecturas."""
    print(
        f"Generando backfill SQL desde {start_date} hasta {end_date} (intervalo {interval_minutes} min)..."
    )
    print(f"Archivo de salida: {output_file}")

    current_time = start_date
    total_rows_generated = 0
    batch_values = []  # Lista para acumular VALUES de un batch INSERT

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"-- Backfill SQL para la tabla '{LECTURAS_TABLE_NAME}'\n")
            f.write(f"-- Generado en: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"-- Rango: {start_date.isoformat()} a {end_date.isoformat()}\n")
            f.write(f"-- Intervalo: {interval_minutes} minutos\n\n")
            # Opcional: Deshabilitar triggers/constraints temporalmente si es necesario (requiere permisos)
            # f.write("SET session_replication_role = 'replica';\n\n")

            start_time_gen = time.time()

            while current_time < end_date:
                if (
                    total_rows_generated > 0
                    and total_rows_generated % (INSERT_BATCH_SIZE * 10) == 0
                ):
                    elapsed = time.time() - start_time_gen
                    print(
                        f"  Generadas {total_rows_generated} filas para {current_time.strftime('%Y-%m-%d %H:%M')}... ({elapsed:.2f}s)"
                    )

                # Simular para todas las escuelas en este intervalo
                for school in config_data:
                    consumo = simular_consumo_determinista(
                        school, current_time, interval_minutes
                    )
                    sede_fk_id = school.get("id_sede")

                    if sede_fk_id is None:
                        continue  # Saltar si falta ID

                    # Columnas: id_sede_fk, timestamp_utc, consumo_kwh, procesado
                    # (id es autoincremental, fecha_recepcion tiene default)
                    value_tuple = (
                        sede_fk_id,
                        current_time,  # Ya es datetime con tzinfo
                        consumo,
                        False,  # 'procesado' inicia en False
                    )
                    # Formatear la tupla para la cláusula VALUES
                    formatted_values = ", ".join(map(format_sql_value, value_tuple))
                    batch_values.append(f"({formatted_values})")
                    total_rows_generated += 1

                    # Escribir el batch INSERT cuando alcance el tamaño deseado
                    if len(batch_values) >= INSERT_BATCH_SIZE:
                        f.write(
                            f"INSERT INTO {LECTURAS_TABLE_NAME} (id_sede_fk, timestamp_utc, consumo_kwh, procesado)\nVALUES\n"
                        )
                        f.write(",\n".join(batch_values))
                        f.write(";\n\n")
                        batch_values = []  # Resetear el batch

                # Avanzar al siguiente intervalo de tiempo
                current_time += timedelta(minutes=interval_minutes)

            # Escribir cualquier valor restante en el último batch
            if batch_values:
                f.write(
                    f"INSERT INTO {LECTURAS_TABLE_NAME} (id_sede_fk, timestamp_utc, consumo_kwh, procesado)\nVALUES\n"
                )
                f.write(",\n".join(batch_values))
                f.write(";\n")

            # Opcional: Rehabilitar triggers/constraints
            # f.write("\nSET session_replication_role = 'origin';\n")

            end_time_gen = time.time()
            print("\nGeneración de SQL completada.")
            print(f"Total filas generadas: {total_rows_generated}")
            print(
                f"Tiempo total de generación: {end_time_gen - start_time_gen:.2f} segundos."
            )

    except Exception as e:
        print(f"\nError durante la generación del SQL de backfill: {e}")
        import traceback

        traceback.print_exc()


# --- Punto de Entrada ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generar SQL para backfill de lecturas de energía."
    )
    parser.add_argument(
        "--fecha_inicio",
        default="2023-01-01T00:00:00Z",
        help="Fecha de inicio UTC (formato ISO 8601: YYYY-MM-DDTHH:MM:SSZ).",
    )
    parser.add_argument(
        "--fecha_fin",
        default="now",
        help="Fecha de fin UTC (formato ISO 8601 o 'now' para la fecha actual).",
    )
    parser.add_argument(
        "--intervalo_minutos",
        type=int,
        default=60,
        help="Intervalo de simulación en minutos.",
    )
    parser.add_argument(
        "--output_file",
        default=DEFAULT_OUTPUT_SQL_FILE,
        help=f"Nombre del archivo SQL de salida (default: {DEFAULT_OUTPUT_SQL_FILE}).",
    )
    parser.add_argument(
        "--config_path",
        default=JSON_CONFIG_PATH,
        help="Ruta al archivo JSON de configuración.",
    )

    args = parser.parse_args()

    # Cargar configuración
    if not os.path.exists(args.config_path):
        print(f"Error: Archivo de configuración '{args.config_path}' no encontrado.")
        exit(1)
    try:
        with open(args.config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)
        if not config_data:
            print(f"Error: El archivo de configuración está vacío o no es válido.")
            exit(1)
    except Exception as e:
        print(f"Error cargando el archivo de configuración: {e}")
        exit(1)

    # Parsear fechas
    try:
        start_dt = datetime.fromisoformat(args.fecha_inicio.replace("Z", "+00:00"))
        if args.fecha_fin.lower() == "now":
            end_dt = datetime.now(timezone.utc)
        else:
            end_dt = datetime.fromisoformat(args.fecha_fin.replace("Z", "+00:00"))

        # Asegurar que las fechas tengan timezone UTC
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)

    except ValueError:
        print("Error: Formato de fecha inválido. Usa YYYY-MM-DDTHH:MM:SSZ o 'now'")
        exit(1)

    # Generar el SQL
    generate_sql_backfill(
        config_data, start_dt, end_dt, args.intervalo_minutos, args.output_file
    )
