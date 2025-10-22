# app/routers/simulation_router.py
import os
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database.database import get_db
from app.schemas import energy_schemas
from app.services import energy_service
from app.simulation.simulation import load_config_data, simular_consumo

router = APIRouter()

# --- Configuración y Carga del JSON de Escuelas ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Ruta al archivo JSON dentro del proyecto desplegado
JSON_CONFIG_PATH = os.path.join(BASE_DIR, "schools_config.json")  # Ajusta esta ruta

_config_data_cache = None

# --- Seguridad (igual que en batch_router) ---
API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == settings.SECRET_KEY:
        return api_key
    else:
        # Intentar leer desde query param como fallback (menos seguro)
        # api_key_query: Optional[str] = Query(None, alias="secret") # No se puede usar Query en Security
        # if api_key_query == settings.SECRET_KEY: return api_key_query
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials for simulation trigger",
        )


# --- Fin Seguridad ---


@router.get(
    "/run_minute",  # El path que Vercel Cron llamará
    summary="Simular y Guardar Lecturas de Energía de un Minuto (Cron)",
    tags=["Simulación Cron"],
    # Proteger el endpoint para que solo Vercel (o alguien con la key) lo llame
    dependencies=[Depends(get_api_key)],
)
def trigger_minute_simulation(db: Session = Depends(get_db)):
    """
    Endpoint llamado por Vercel Cron cada minuto.
    Simula lecturas para todas las sedes y las guarda directamente.
    """
    print(
        f"[{datetime.now(timezone.utc).isoformat()}] Iniciando simulación por minuto..."
    )
    try:
        config_data = load_config_data()
        if not config_data:
            raise HTTPException(
                status_code=500, detail="Configuración de escuelas no disponible."
            )

        now_utc = datetime.now(timezone.utc)
        # Genera el formato ISO estándar
        timestamp_utc_str = now_utc.isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
        if not timestamp_utc_str.endswith("Z"):
            timestamp_utc_str += "Z"
        # Re-parsear para asegurar que es un objeto datetime para Pydantic
        timestamp_dt = datetime.fromisoformat(timestamp_utc_str.replace("Z", "+00:00"))

        current_batch_payload: List[energy_schemas.EnergyReadingCreateSchema] = []
        intervalo_minutos = 1  # Simulación para el último minuto

        for school in config_data:
            # Usar la lógica de simulación importada
            consumo = simular_consumo(school, now_utc, intervalo_minutos)
            sede_id = school.get("id_sede")
            if sede_id is None:
                continue  # Saltar si falta ID

            # Crear el objeto schema Pydantic directamente
            payload_schema = energy_schemas.EnergyReadingCreateSchema(
                ID_Sensor=int(sede_id),  # Asegurar que sea int
                Nombre_Sede=school.get("nombre_sede"),
                ID_Localidad=school.get(
                    "localidad", "Desconocida"
                ),  # Default por si acaso
                TimestampUTC=timestamp_dt,  # Usar el objeto datetime parseado
                Consumo_kWh=consumo,
                lat=school.get("lat"),
                lon=school.get("lon"),
            )
            current_batch_payload.append(payload_schema)

        num_simulated = len(current_batch_payload)
        if not current_batch_payload:
            print("No se generaron lecturas simuladas (configuración vacía?).")
            return {"status": "success", "message": "No readings simulated."}

        print(f"  - {num_simulated} lecturas simuladas para {timestamp_utc_str}.")

        # Llamar DIRECTAMENTE al servicio de ingesta
        inserted_count = energy_service.save_readings_batch(
            db=db, readings=current_batch_payload
        )

        print(f"  - {inserted_count} lecturas guardadas por el servicio.")

        if inserted_count != num_simulated:
            # Loggear discrepancia pero no necesariamente fallar la ejecución del cron
            print(
                f"ADVERTENCIA: Se simularon {num_simulated} pero se guardaron {inserted_count}."
            )

        print(
            f"[{datetime.now(timezone.utc).isoformat()}] Finalizada simulación por minuto."
        )
        return {
            "status": "success",
            "message": f"Simulated {num_simulated}, Saved {inserted_count} readings.",
        }

    except FileNotFoundError as fnf:
        print(f"ERROR CRÍTICO en simulación: {fnf}")
        raise HTTPException(status_code=500, detail=str(fnf))
    except IOError as ioe:
        print(f"ERROR CRÍTICO en simulación: {ioe}")
        raise HTTPException(status_code=500, detail=str(ioe))
    except Exception as e:
        print(f"Error inesperado en trigger_minute_simulation: {e}")
        import traceback

        traceback.print_exc()  # Loggear el traceback completo
        raise HTTPException(
            status_code=500, detail=f"Error interno en simulación: {str(e)}"
        )
