import json
import os
import random
from datetime import datetime

# --- Configuración y Carga del JSON de Escuelas ---
# Obtener el directorio donde está ESTE archivo (logic.py)
SIMULATION_DIR = os.path.dirname(os.path.abspath(__file__))
# Subir un nivel para estar en 'app' y luego encontrar el JSON
APP_DIR = os.path.dirname(SIMULATION_DIR)
JSON_CONFIG_PATH = os.path.join(APP_DIR, "schools_config.json")  # Ruta dentro de app/

_config_data_cache = None


def load_config_data():
    """Carga (y cachea) la configuración de escuelas desde app/schools_config.json."""
    global _config_data_cache
    if _config_data_cache is None:
        if not os.path.exists(JSON_CONFIG_PATH):
            print(f"ERROR CRÍTICO: No se encontró {JSON_CONFIG_PATH}")
            raise FileNotFoundError(
                f"Archivo de configuración no encontrado: {JSON_CONFIG_PATH}"
            )
        try:
            with open(JSON_CONFIG_PATH, "r", encoding="utf-8") as f:
                _config_data_cache = json.load(f)
            print(
                f"Configuración de escuelas cargada desde: {JSON_CONFIG_PATH}"
            )  # Mensaje de depuración
        except Exception as e:
            print(f"ERROR CRÍTICO: Fallo al cargar/parsear {JSON_CONFIG_PATH}: {e}")
            raise IOError(f"Error cargando configuración: {e}")
    return _config_data_cache


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
