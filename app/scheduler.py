# back/app/scheduler.py
import asyncio
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from app.database.database import SessionLocal
from app.schemas import energy_schemas
from app.services import batch_processing_service, energy_service
from app.simulation.simulation import load_config_data, simular_consumo

scheduler = AsyncIOScheduler(timezone="UTC")

# --- Funciones que ejecutarán los Jobs ---
# ¡Importante! Los jobs necesitan su propia sesión de BD


async def run_hourly_job():
    """Tarea que ejecuta el batch horario."""
    print(f"[{datetime.now(timezone.utc).isoformat()}] Ejecutando Job Horario...")
    db: Session = SessionLocal()
    try:
        now_utc = datetime.now(timezone.utc)
        result = batch_processing_service.process_hourly_batch(
            db=db, target_hour_utc=now_utc
        )
        print(f"Resultado Job Horario: {result}")
    except Exception as e:
        print(f"ERROR en Job Horario: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()  # Asegurar cierre de sesión
    print(f"[{datetime.now(timezone.utc).isoformat()}] Finalizado Job Horario.")


async def run_simulation_job():
    """Tarea que simula y guarda lecturas."""
    print(f"[{datetime.now(timezone.utc).isoformat()}] Ejecutando Job Simulación...")
    db: Session = SessionLocal()
    try:
        config_data = load_config_data()  # Cargar config de escuelas
        if not config_data:
            print("Error en Job Simulación: No se pudo cargar config_data.")
            return  # Salir si no hay config

        now_utc = datetime.now(timezone.utc)
        timestamp_dt = now_utc  # Usar el objeto datetime

        current_batch_payload = []
        intervalo_minutos = 2  # El intervalo simulado (aunque el job corre cada 2 min)

        for school in config_data:
            consumo = simular_consumo(school, now_utc, intervalo_minutos)
            sede_id = school.get("id_sede")
            if sede_id is None:
                continue

            # Crear el schema directamente para validación implícita
            try:
                payload_schema = energy_schemas.EnergyReadingCreateSchema(
                    ID_Sensor=int(sede_id),
                    Nombre_Sede=school.get("nombre_sede"),
                    ID_Localidad=school.get("localidad", "Desconocida"),
                    TimestampUTC=timestamp_dt,
                    Consumo_kWh=consumo,
                    lat=school.get("lat"),
                    lon=school.get("lon"),
                )
                current_batch_payload.append(payload_schema)
            except Exception as pydantic_err:
                # Loggear si un registro individual falla la validación Pydantic
                print(
                    f"Error Pydantic en Job Simulación para sede {sede_id}: {pydantic_err}"
                )
                continue  # Saltar este registro

        num_simulated = len(current_batch_payload)
        if not current_batch_payload:
            print("Job Simulación: No se generaron lecturas.")
            return

        print(f"  - {num_simulated} lecturas simuladas.")

        # Llamar al servicio de ingesta
        inserted_count = energy_service.save_readings_batch(
            db=db, readings=current_batch_payload
        )
        print(f"  - {inserted_count} lecturas guardadas.")

    except Exception as e:
        print(f"ERROR en Job Simulación: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()  # Asegurar cierre de sesión
    print(f"[{datetime.now(timezone.utc).isoformat()}] Finalizado Job Simulación.")



async def run_daily_job():
    """Tarea que ejecuta el batch diario para el día anterior."""
    print(f"[{datetime.now(timezone.utc).isoformat()}] Ejecutando Job Diario...")
    db: Session = SessionLocal()
    try:
        yesterday_utc_date = datetime.now(timezone.utc).date() - timedelta(days=1)
        result = batch_processing_service.process_daily_batch(db=db, target_date_utc=yesterday_utc_date)
        print(f"Resultado Job Diario: {result}")
    except Exception as e:
        print(f"ERROR en Job Diario: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Finalizado Job Diario.")


# --- Función para iniciar el planificador ---
def start_scheduler():
    print("Iniciando planificador APScheduler...")
    try:
        # Añadir el job horario (ejecutar a los 5 minutos de cada hora)
        scheduler.add_job(
            run_hourly_job,
            trigger=CronTrigger(minute="5", timezone="UTC"),  # Cada hora a los 5 min
            id="hourly_batch_job",
            name="Batch Horario de Consumo",
            replace_existing=True,
        )

        # Añadir el job de simulación (ejecutar cada 5 minutos)
        scheduler.add_job(
            run_simulation_job,
            trigger=CronTrigger(minute="*/5", timezone="UTC"),
            id="simulation_job",
            name="Simulador de Lecturas",
            replace_existing=True,
        )

         # Ejecutar cada día a las 00:15 UTC (procesará el día anterior)
        scheduler.add_job(
             run_daily_job,
             trigger=CronTrigger(hour="0", minute="15", timezone="UTC"),
             id="daily_batch_job",
             name="Batch Diario de Consumo",
             replace_existing=True
        )

        scheduler.start()
        print("Planificador APScheduler iniciado con los jobs.")
        print("Jobs actuales:", scheduler.get_jobs())

    except Exception as e:
        print(f"Error al iniciar APScheduler: {e}")
        # Considera detener la aplicación si el scheduler es crítico
        raise e  # Propagar el error


# --- Función para detener el planificador ---
def stop_scheduler():
    if scheduler.running:
        print("Deteniendo planificador APScheduler...")
        scheduler.shutdown()
        print("Planificador APScheduler detenido.")
