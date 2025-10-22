import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import requests

# Obtener el directorio donde se ejecuta el script
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

# --- Constantes y Configuración ---
JSON_COLEGIOS = 'schools_config.json'
# Endpoint de la API para enviar datos
API_ENDPOINT = "http://localhost:8000/api/energy_data/"

# --- Funciones de Simulación "Inteligente" ---

def get_factores_simu(timestamp):
    """
    Calcula factores de ajuste basados en la hora, día, mes y tendencia.
    """
    factors = {'time': 1.0, 'day': 1.0, 'month': 1.0, 'trend': 1.0}
    hour = timestamp.hour
    day_of_week = timestamp.weekday() # Lunes=0, Domingo=6
    month = timestamp.month
    year_diff = timestamp.year - 2023

    # 1. Factor por Hora del Día (Mayor consumo en horario escolar)
    if 7 <= hour < 17: # Horario escolar principal
        factors['time'] = random.uniform(1.5, 2.5) # Pico de consumo
    elif 17 <= hour < 21: # Actividades extra o limpieza
        factors['time'] = random.uniform(0.8, 1.2)
    else: # Noche y madrugada
        factors['time'] = random.uniform(0.1, 0.4) # Consumo mínimo (standby, seguridad)

    # 2. Factor por Día de la Semana (Menor consumo fines de semana)
    if day_of_week >= 5: # Sábado o Domingo
        factors['day'] = random.uniform(0.2, 0.5) # Mucho menor consumo
    else: # Lunes a Viernes
         factors['day'] = random.uniform(0.95, 1.05)

    # 3. Factor por Mes (Vacaciones, clima - simplificado)
    if month in [12, 1, 6, 7]: # Meses con posibles vacaciones largas
        factors['month'] = random.uniform(0.4, 0.7)

    # 4. Factor de Tendencia Anual (Ligero aumento/disminución desde 2023)
    # Asumir un ligero aumento anual del 1% en consumo
    factors['trend'] = 1.0 + (0.01 * year_diff)

    return factors

def simular_consumo(school_info, timestamp, intervalo_minutos):
    """
    Simula el consumo para una escuela en un timestamp dado para un intervalo específico.
    """
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

# --- Función para enviar datos ---
def enviar_api(batch_payload): # Acepta una lista (lote)
    """Envia un lote de datos (lista de payloads) JSON al endpoint."""
    # No enviar si el lote está vacío
    if not batch_payload: 
        print("Lote vacío, no se envía nada.")
        return True

    # La librería `requests` se encarga de serializarla a un array JSON
    try:
        response = requests.post(API_ENDPOINT, json=batch_payload, timeout=30)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        # El error aplica a todo el lote
        print(f"Error enviando lote de {len(batch_payload)} registros: {e}")
        return False
    except Exception as e:
        print(f"Error inesperado en enviar_api enviando lote: {e}")
        return False


# --- Modos de ejecución ---

def generar_historico(config_data, fecha_inicio, end_date, intervalo_minutos):
    """Genera y envía datos históricos en lotes."""
    print(f"Iniciando historico en lotes desde {fecha_inicio} hasta {end_date} con intervalo de {intervalo_minutos} minutos...")
    current_time = fecha_inicio
    total_records_sent = 0
    total_batches_sent = 0
    total_batches_failed = 0

    while current_time < end_date:
        # Asegura que el datetime tenga timezone UTC
        if isinstance(current_time, datetime) and current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)

        # Genera el formato ISO estándar reemplazando el offset si existe y añadiendo Z
        if isinstance(current_time, datetime):
            timestamp_utc_str = current_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        # Por si en algún punto la cadena no termina con Z
        if not timestamp_utc_str.endswith('Z'):
            timestamp_utc_str += 'Z'

        # Crear lista para acumular lecturas de este intervalo
        current_batch_payload = []

        # Simular para todas las escuelas en este intervalo
        for school in config_data:
            consumo = simular_consumo(school, current_time, intervalo_minutos)
            payload = {
                "ID_Sensor": school['id_sede'],
                "Nombre_Sede": school['nombre_sede'],
                "ID_Localidad": school['localidad'],
                "TimestampUTC": timestamp_utc_str,
                "Consumo_kWh": consumo,
                "lat": school.get('lat', None),
                "lon": school.get('lon', None)
            }
            # Añadir la lectura individual al lote actual
            current_batch_payload.append(payload)

        # Enviar el lote completo después de iterar todas las escuelas
        if current_batch_payload: # Asegurarse de no enviar lotes vacíos
            if enviar_api(current_batch_payload):
                total_records_sent += len(current_batch_payload)
                total_batches_sent += 1
                print(f"  Lote para {timestamp_utc_str} enviado | Registros: {len(current_batch_payload)} | Lotes OK: {total_batches_sent}")
            else:
                total_batches_failed += 1
                # Mensaje de log actualizado
                print(f"  ERROR enviando lote para {timestamp_utc_str} | Registros: {len(current_batch_payload)} | Lotes Fallidos: {total_batches_failed}")
            # Pequeña pausa entre lotes, si se desea (puede no ser necesaria)
            # time.sleep(0.05)

        # Avanzar al siguiente intervalo de tiempo
        current_time += timedelta(minutes=intervalo_minutos)

    print("\nhistorico completado.")
    print(f"Total lotes enviados con éxito: {total_batches_sent}")
    print(f"Total lotes fallidos: {total_batches_failed}")
    print(f"Total registros individuales enviados (aprox): {total_records_sent}")


def generar_realtime(config_data, intervalo_minutos):
    """Genera y envía datos periódicamente en 'tiempo real' en lotes."""
    print(f"Iniciando simulación en tiempo real en lotes cada {intervalo_minutos} minutos...")
    interval_seconds = intervalo_minutos * 60
    while True:
        start_loop_time = time.time()
        now_utc = datetime.now(timezone.utc)
        timestamp_utc_str = now_utc.isoformat(timespec='milliseconds') + 'Z'
        print(f"\n--- Generando lote para {timestamp_utc_str} ---")

        if isinstance(now_utc, datetime) and now_utc.tzinfo is None: # Para realtime
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        # Genera el formato ISO estándar reemplazando el offset si existe y añadiendo Z
        timestamp_utc_str = now_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        # Si en algún punto la cadena no termina con Z (poco probable con .replace), añádela
        if not timestamp_utc_str.endswith('Z'):
            timestamp_utc_str += 'Z'

        # Crear lista para acumular lecturas de este intervalo
        current_batch_payload = []

        # Simular para todas las escuelas en este intervalo
        for school in config_data:
            consumo = simular_consumo(school, now_utc, intervalo_minutos)
            payload = {
                "ID_Sensor": int(school['id_sede']),
                "Nombre_Sede": school['nombre_sede'],
                "ID_Localidad": school['localidad'],
                "TimestampUTC": timestamp_utc_str,
                "Consumo_kWh": consumo,
                "lat": school.get('lat', None),
                "lon": school.get('lon', None)
            }
            # Añadir la lectura individual al lote actual
            current_batch_payload.append(payload)

        # Enviar el lote completo
        records_in_batch = len(current_batch_payload)
        batch_sent_ok = False
        if current_batch_payload:
            batch_sent_ok = enviar_api(current_batch_payload)

        # Loggear resultado del lote
        if batch_sent_ok:
             print(f"--- Lote para {timestamp_utc_str} procesado | Éxito | Registros: {records_in_batch} ---")
        else:
             print(f"--- LOTE para {timestamp_utc_str} FALLIDO | Registros: {records_in_batch} ---")


        # Esperar hasta el próximo intervalo
        elapsed_time = time.time() - start_loop_time
        sleep_time = max(0, interval_seconds - elapsed_time)
        print(f"Esperando {sleep_time:.2f} segundos para el próximo ciclo...")
        time.sleep(sleep_time)


# --- Punto de Entrada Principal ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador de Consumo Energético de Colegios")
    parser.add_argument('mode', choices=['preprocess', 'historico', 'realtime'],
                        help="Modo de operación: 'preprocess' para generar config JSON, "
                             "'historico' para generar datos históricos, 'realtime' para simulación continua.")
    parser.add_argument('--fecha_inicio', default='2023-01-01T00:00:00',
                        help="Fecha de inicio para histórico (formato YYYY-MM-DDTHH:MM:SS).")
    parser.add_argument('--intervalo_minutos', type=int, default=60, # Intervalo de 1 hora por defecto
                        help="Intervalo de simulación en minutos (para histórico y realtime).")
    parser.add_argument('--csv_path', default='export (1).csv', help="Ruta al archivo CSV de entrada.")
    parser.add_argument('--config_path', default='schools_config.json', help="Ruta al archivo JSON de configuración.")
    parser.add_argument('--api_url', default=API_ENDPOINT, help="URL del endpoint de la API para recibir datos.")

    args = parser.parse_args()

    # Actualizar URL de la API si se proporciona como argumento
    API_ENDPOINT = args.api_url

    if args.mode == 'preprocess':
        # Importar la función de preprocesamiento si este archivo se ejecuta directamente
        from preprocess_schools import preprocess_school_data
        preprocess_school_data(args.csv_path, args.config_path)

    elif args.mode in ['historico', 'realtime']:
        # Cargar configuración
        if not os.path.exists(args.config_path):
             print(f"Error: Archivo de configuración '{args.config_path}' no encontrado.")
             print("Ejecuta primero el modo 'preprocess' o asegúrate de que el archivo existe.")
             exit(1)
        try:
            with open(args.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            if not config_data:
                 print(f"Error: El archivo de configuración '{args.config_path}' está vacío o no es válido.")
                 exit(1)
        except Exception as e:
            print(f"Error cargando el archivo de configuración: {e}")
            exit(1)


        if args.mode == 'historico':
            try:
                start_dt = datetime.fromisoformat(args.fecha_inicio).replace(tzinfo=timezone.utc)
                end_dt = datetime.now(timezone.utc) # Hasta el momento actual
                generar_historico(config_data, start_dt, end_dt, args.intervalo_minutos)
            except ValueError:
                 print("Error: Formato de fecha de inicio inválido. Usa YYYY-MM-DDTHH:MM:SS")
                 exit(1)
        elif args.mode == 'realtime':
            generar_realtime(config_data, args.intervalo_minutos)