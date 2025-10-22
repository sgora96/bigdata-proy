# app/services/batch_processing_service.py
from datetime import date, datetime, timedelta, timezone  # Añadir date; Asegurar date
from typing import Optional  # Añadir Optional

from sqlalchemy import Date as SQLDate
from sqlalchemy import cast, func, select  # Añadir select, func
from sqlalchemy.orm import Session

from app.models import energy_models  # Importar modelos para consulta directa
from app.repositories import energy_repository


def process_hourly_batch(db: Session, target_hour_utc: datetime):
    """
    Procesa el batch horario
    """
    end_process_hour = target_hour_utc.replace(minute=0, second=0, microsecond=0)
    start_process_hour = end_process_hour - timedelta(hours=1)

    print(
        f"\n[BATCH HORARIO] Iniciando procesamiento para la hora: {start_process_hour.strftime('%Y-%m-%d %H:%M')} a {end_process_hour.strftime('%Y-%m-%d %H:%M')}"
    )

    try:
        # 1. Calcular agregados (GROUP BY id_sede_fk)
        print("  - Calculando agregados horarios...")
        aggregates = energy_repository.aggregate_hourly_consumption(
            db=db, start_hour_utc=start_process_hour, end_hour_utc=end_process_hour
        )

        if not aggregates:
            print("  - No se encontraron lecturas crudas no procesadas para esta hora.")
            # Intentar marcar de todas formas por si falló antes
            energy_repository.mark_readings_as_processed(
                db, start_process_hour, end_process_hour
            )
            db.commit()  # Commit del marcado (si no hubo agregados)
            print("[BATCH HORARIO] Finalizado (sin nuevos agregados).")
            return {"status": "success", "message": "No new readings to process."}

        print(f"  - {len(aggregates)} agregados calculados.")

        # 2. Preparar guardado de agregados (sin commit aún)
        energy_repository.save_hourly_aggregates(
            db=db, aggregates=aggregates, hour_start_utc=start_process_hour
        )

        # 3. Preparar marcado de lecturas crudas como procesadas (sin commit aún)
        affected_rows = energy_repository.mark_readings_as_processed(
            db=db, start_hour_utc=start_process_hour, end_hour_utc=end_process_hour
        )

        # 4. Hacer COMMIT de la transacción (guardar agregados + marcar crudos)
        db.commit()
        print(
            f"  - Commit exitoso. Agregados guardados: {len(aggregates)}, Crudos marcados: {affected_rows}"
        )
        print("[BATCH HORARIO] Procesamiento completado con éxito.")
        return {
            "status": "success",
            "message": f"Processed {len(aggregates)} aggregates for hour {start_process_hour}.",
        }

    except Exception as e:
        print(f"[BATCH HORARIO] ERROR durante el procesamiento: {e}")
        db.rollback()  # Asegurar rollback si algo falla
        return {"status": "error", "message": str(e)}


def find_last_processed_hour(db: Session) -> Optional[datetime]:
    """Encuentra el timestamp de inicio de la última hora procesada en ConsumoHora."""
    stmt = select(func.max(energy_models.ConsumoHora.hora_inicio_utc))
    result = db.execute(stmt)
    last_hour = result.scalar_one_or_none()
    return last_hour


def find_first_unprocessed_reading_time(db: Session) -> Optional[datetime]:
    """Encuentra el timestamp de la lectura cruda más antigua no procesada."""
    stmt = select(func.min(energy_models.LecturaEnergia.timestamp_utc)).where(
        ~energy_models.LecturaEnergia.procesado
    )
    result = db.execute(stmt)
    first_time = result.scalar_one_or_none()
    return first_time


def run_hourly_catchup(db: Session):
    """
    Procesa todas las horas pendientes (adaptado).
    """
    print("\n[BATCH CATCHUP] Iniciando procesamiento de horas pendientes...")
    last_processed_start_hour = find_last_processed_hour(db)
    first_unprocessed_time = find_first_unprocessed_reading_time(db)

    if not first_unprocessed_time:
        print("[BATCH CATCHUP] No hay lecturas crudas no procesadas. Finalizado.")
        return

    # Determinar la primera hora a procesar
    if last_processed_start_hour:
        # La siguiente hora a procesar es la que viene después de la última guardada
        next_hour_to_process = last_processed_start_hour + timedelta(hours=1)
        # No podemos empezar antes de la primera lectura disponible
        first_possible_hour = first_unprocessed_time.replace(
            minute=0, second=0, microsecond=0
        )
        if next_hour_to_process < first_possible_hour:
            next_hour_to_process = first_possible_hour
    else:
        # Empezar desde la hora de la primera lectura cruda
        next_hour_to_process = first_unprocessed_time.replace(
            minute=0, second=0, microsecond=0
        )

    # Determinar la última hora que podemos procesar COMPLETAMENTE
    latest_reading_time_stmt = select(
        func.max(energy_models.LecturaEnergia.timestamp_utc)
    )
    latest_reading_time = db.execute(latest_reading_time_stmt).scalar_one_or_none()

    if not latest_reading_time:
        print(
            "[BATCH CATCHUP] No se encontró la hora de la última lectura cruda. Finalizado."
        )
        return

    # Procesamos hasta la hora ANTERIOR a la que contiene la última lectura
    last_full_hour_to_process = latest_reading_time.replace(
        minute=0, second=0, microsecond=0
    )

    print(
        f"[BATCH CATCHUP] Rango a procesar: Desde {next_hour_to_process} hasta ANTES de {last_full_hour_to_process}"
    )

    # Iterar y procesar cada hora pendiente
    current_hour = next_hour_to_process
    total_processed_count = 0
    max_hours_per_run = 99999 * 7  # Límite opcional para no sobrecargar una ejecución
    hours_processed_this_run = 0

    while (
        current_hour < last_full_hour_to_process
        and hours_processed_this_run < max_hours_per_run
    ):
        print(
            f"\n[BATCH CATCHUP] Procesando catchup para la hora: {current_hour.strftime('%Y-%m-%d %H:%M')}..."
        )
        # Llamamos a process_hourly_batch pasando la hora SIGUIENTE para que procese 'current_hour'
        result = process_hourly_batch(
            db=db, target_hour_utc=current_hour + timedelta(hours=1)
        )
        if result.get("status") == "success":
            total_processed_count += 1
            hours_processed_this_run += 1
        else:
            print(
                f"[BATCH CATCHUP] ERROR procesando la hora {current_hour}. Deteniendo catchup."
            )
            break  # Detener si un batch falla
        current_hour += timedelta(hours=1)

    if hours_processed_this_run >= max_hours_per_run:
        print(
            f"[BATCH CATCHUP] Límite de {max_hours_per_run} horas por ejecución alcanzado."
        )

    print(
        f"\n[BATCH CATCHUP] Finalizado. Se procesaron {total_processed_count} horas en esta ejecución."
    )


# Procesamiento diario

def process_daily_batch(db: Session, target_date_utc: date):
    """
    Procesa el batch diario para una fecha específica agregando desde datos horarios.
    """
    print(f"\n[BATCH DIARIO] Iniciando procesamiento para el día: {target_date_utc.strftime('%Y-%m-%d')}")

    try:
        # 1. Calcular agregados diarios desde la tabla horaria
        print("  - Calculando agregados diarios desde datos horarios...")
        aggregates = energy_repository.aggregate_daily_consumption_from_hourly(
            db=db,
            target_date_utc=target_date_utc
        )

        if not aggregates:
            print("  - No se encontraron datos horarios para agregar para este día.")
            # Considerar si quieres insertar registros diarios con 0 consumo o no.
            # Por ahora, simplemente no hacemos nada si no hay datos horarios.
            print("[BATCH DIARIO] Finalizado (sin agregados que guardar).")
            return {"status": "success", "message": "No hourly data found to aggregate for this day."}

        print(f"  - {len(aggregates)} agregados diarios calculados.")

        # 2. Guardar los agregados diarios (esto hará commit internamente en el repo si no cambiamos eso)
        # O mejor, el servicio maneja el commit
        energy_repository.save_daily_aggregates(
            db=db,
            aggregates=aggregates,
            target_date_utc=target_date_utc
        )

        # 3. Hacer Commit de la transacción
        db.commit()
        print(f"  - Commit exitoso. Agregados diarios guardados para {target_date_utc.strftime('%Y-%m-%d')}")
        print("[BATCH DIARIO] Procesamiento completado con éxito.")
        return {"status": "success", "message": f"Processed {len(aggregates)} daily aggregates for {target_date_utc}."}

    except Exception as e:
        print(f"[BATCH DIARIO] ERROR durante el procesamiento para {target_date_utc}: {e}")
        db.rollback()
        return {"status": "error", "message": str(e)}

def run_daily_catchup(db: Session):
    """
    Procesa todos los días pendientes desde el último procesado en ConsumoDia
    hasta el último día disponible en ConsumoHora.
    """
    print("\n[BATCH CATCHUP DIARIO] Iniciando procesamiento de días pendientes...")
    last_processed_day = energy_repository.find_last_processed_day(db)
    last_day_in_hourly = energy_repository.find_last_date_in_hourly(db)

    if not last_day_in_hourly:
        print("[BATCH CATCHUP DIARIO] No hay datos horarios procesados para agregar. Finalizado.")
        return

    # Determinar el primer día a procesar
    if last_processed_day:
        next_day_to_process = last_processed_day + timedelta(days=1)
    else:
        # Si nunca se procesó, encontrar la primera fecha en consumo_hora
        first_hourly_date_stmt = select(func.min(cast(energy_models.ConsumoHora.hora_inicio_utc, SQLDate)))
        first_hourly_date = db.execute(first_hourly_date_stmt).scalar_one_or_none()
        if not first_hourly_date:
             print("[BATCH CATCHUP DIARIO] No se encontró fecha de inicio en datos horarios. Finalizado.")
             return
        next_day_to_process = first_hourly_date

    # El último día a procesar es el último día completo que existe en la tabla horaria
    last_day_to_process = last_day_in_hourly

    print(f"[BATCH CATCHUP DIARIO] Rango a procesar: Desde {next_day_to_process} hasta {last_day_to_process}")

    current_day = next_day_to_process
    total_processed_count = 0
    max_days_per_run = 1000000 # Límite opcional para no sobrecargar una ejecución
    days_processed_this_run = 0

    while current_day <= last_day_to_process and days_processed_this_run < max_days_per_run:
        print(f"\n[BATCH CATCHUP DIARIO] Procesando catchup para el día: {current_day.strftime('%Y-%m-%d')}...")
        result = process_daily_batch(db=db, target_date_utc=current_day)
        if result.get("status") == "success":
            total_processed_count += 1
            days_processed_this_run += 1
        else:
            print(f"[BATCH CATCHUP DIARIO] ERROR procesando el día {current_day}. Deteniendo catchup.")
            break # Detener si un batch falla
        current_day += timedelta(days=1)

    if days_processed_this_run >= max_days_per_run:
         print(f"[BATCH CATCHUP DIARIO] Límite de {max_days_per_run} días por ejecución alcanzado.")

    print(f"\n[BATCH CATCHUP DIARIO] Finalizado. Se procesaron {total_processed_count} días en esta ejecución.")