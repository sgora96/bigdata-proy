# app/repositories/energy_repository.py
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

from sqlalchemy import Date as SQLDate
from sqlalchemy import cast, func, select, update
from sqlalchemy.orm import Session, joinedload

from app.models import energy_models
from app.schemas import energy_schemas


def create_energy_readings_batch(
    db: Session,
    readings_data: List[Tuple[energy_schemas.EnergyReadingCreateSchema, int]],
) -> int:
    """
    Inserta un lote de lecturas de energía crudas usando la FK de la sede.
    'readings_data' es una lista de tuplas: (datos_lectura_original, id_sede_fk_obtenido)
    Retorna el número de registros insertados.
    """
    db_readings = []
    for reading_in, sede_fk_id in readings_data:  # Desempaquetar la tupla
        # Convertir el schema Pydantic a un modelo SQLAlchemy
        db_reading = energy_models.LecturaEnergia(
            # --- USAR LA FK ---
            id_sede_fk=sede_fk_id,
            # --- FIN USO FK ---
            timestamp_utc=reading_in.TimestampUTC,
            consumo_kwh=reading_in.Consumo_kWh,
            procesado=False,
            # Los campos redundantes ya no están en el modelo LecturaEnergia
        )
        db_readings.append(db_reading)

    try:
        db.add_all(db_readings)
        # El commit lo debe manejar el servicio que orquesta la operación completa
        # db.commit() # Comentado
        return len(db_readings)
    except Exception as e:
        print(f"Error en BD al preparar inserción de lote crudo: {e}")
        # db.rollback() # El rollback también lo maneja el servicio
        raise e


def aggregate_hourly_consumption(
    db: Session, start_hour_utc: datetime, end_hour_utc: datetime
) -> List[Tuple]:
    """
    Calcula el consumo agregado por hora desde la BD usando GROUP BY id_sede_fk.
    Retorna lista de tuplas: (id_sede_fk, total_kwh, count, avg_kwh)
    """
    sum_consumo = func.sum(energy_models.LecturaEnergia.consumo_kwh).label("total_kwh")
    count_lecturas = func.count(energy_models.LecturaEnergia.id).label("count")
    avg_consumo = func.avg(energy_models.LecturaEnergia.consumo_kwh).label("avg_kwh")

    stmt = (
        select(
            # --- Seleccionar solo la FK y los agregados ---
            energy_models.LecturaEnergia.id_sede_fk,
            sum_consumo,
            count_lecturas,
            avg_consumo,
        )
        .where(
            energy_models.LecturaEnergia.timestamp_utc >= start_hour_utc,
            energy_models.LecturaEnergia.timestamp_utc < end_hour_utc,
            ~energy_models.LecturaEnergia.procesado,
        )
        .group_by(
            # --- Agrupar solo por la FK ---
            energy_models.LecturaEnergia.id_sede_fk
        )
    )

    result = db.execute(stmt)
    return result.all()  # Retorna lista de Rows/Tuplas


def save_hourly_aggregates(
    db: Session, aggregates: List[Tuple], hour_start_utc: datetime
):
    """
    Guarda los registros agregados por hora en la tabla 'consumo_hora'.
    'aggregates' es la lista de tuplas retornada por aggregate_hourly_consumption.
    """
    db_aggregates = []
    now_utc = datetime.now(timezone.utc)
    for agg_row in aggregates:
        db_agg = energy_models.ConsumoHora(
            # --- Usar la FK ---
            id_sede_fk=agg_row.id_sede_fk,
            # --- Fin Uso FK ---
            hora_inicio_utc=hour_start_utc,
            consumo_total_kwh=agg_row.total_kwh,
            numero_lecturas=agg_row.count,
            consumo_promedio_kwh=agg_row.avg_kwh,
            fecha_procesamiento_utc=now_utc,
            # Ya no se guardan nombre_sede, id_localidad aquí
        )
        db_aggregates.append(db_agg)

    if db_aggregates:
        try:
            db.add_all(db_aggregates)
            # Commit manejado por el servicio
            print(
                f"  - Preparados {len(db_aggregates)} registros agregados para la hora {hour_start_utc.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except Exception as e:
            print(f"Error en BD al preparar guardado de agregados horarios: {e}")
            raise e


def mark_readings_as_processed(
    db: Session, start_hour_utc: datetime, end_hour_utc: datetime
) -> int:
    """
    Actualiza 'procesado' a True para lecturas crudas en el intervalo. (Sin cambios)
    """
    stmt = (
        update(energy_models.LecturaEnergia)
        .where(
            energy_models.LecturaEnergia.timestamp_utc >= start_hour_utc,
            energy_models.LecturaEnergia.timestamp_utc < end_hour_utc,
            ~energy_models.LecturaEnergia.procesado,
        )
        .values(procesado=True)
    )
    try:
        result = db.execute(stmt)
        # Commit manejado por el servicio
        affected_rows = result.rowcount
        print(
            f"  - Preparadas para marcar {affected_rows} lecturas crudas como procesadas para la hora {start_hour_utc.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        return affected_rows
    except Exception as e:
        print(f"Error en BD al preparar marcado de lecturas como procesadas: {e}")
        raise e


# --- Función para Consultar Datos Agregados ---
def get_hourly_consumption(
    db: Session,
    start_time: datetime,
    end_time: datetime,
    id_localidad: Optional[str] = None,
    id_sede: Optional[int] = None,
) -> List[energy_models.ConsumoHora]:
    """
    Consulta datos de ConsumoHora. Si se filtra por localidad, requiere JOIN.
    """
    stmt = (
        select(energy_models.ConsumoHora)
        .options(joinedload(energy_models.ConsumoHora.sede))
        .where(
            energy_models.ConsumoHora.hora_inicio_utc >= start_time,
            energy_models.ConsumoHora.hora_inicio_utc < end_time,
        )
        .order_by(energy_models.ConsumoHora.hora_inicio_utc)
    )

    # Aplicar filtros
    if id_sede:
        stmt = stmt.where(energy_models.ConsumoHora.id_sede_fk == id_sede)

    # Si se filtra por localidad, necesitamos unir con la tabla Sedes
    if id_localidad:
        stmt = stmt.join(energy_models.Sede).where(
            energy_models.Sede.id_localidad == id_localidad
        )

    result = db.execute(stmt)
    return result.scalars().all()  # Retorna objetos ConsumoHora


# --- Batch diario ---

def aggregate_daily_consumption_from_hourly(
    db: Session, target_date_utc: date
) -> List[Tuple]:
    """
    Calcula el consumo agregado diario DESDE la tabla 'consumo_hora'
    para una fecha específica usando SQL GROUP BY.
    Retorna lista de tuplas: (id_sede_fk, total_daily_kwh, hour_count)
    """
    # Funciones de agregación
    sum_total_diario = func.sum(energy_models.ConsumoHora.consumo_total_kwh).label(
        "total_daily_kwh"
    )
    count_horas = func.count(energy_models.ConsumoHora.id).label("hour_count")
    # El promedio horario se puede calcular después o aquí, pero count_horas es más útil

    # Convertir la fecha target a datetime de inicio y fin para la consulta horaria
    start_dt = datetime.combine(
        target_date_utc, datetime.min.time(), tzinfo=timezone.utc
    )
    end_dt = start_dt + timedelta(days=1)

    stmt = (
        select(energy_models.ConsumoHora.id_sede_fk, sum_total_diario, count_horas)
        .where(
            energy_models.ConsumoHora.hora_inicio_utc >= start_dt,
            energy_models.ConsumoHora.hora_inicio_utc < end_dt,
            # No necesitamos filtrar por 'procesado' aquí, asumimos que consumo_hora está listo
        )
        .group_by(energy_models.ConsumoHora.id_sede_fk)
    )

    result = db.execute(stmt)
    return result.all()  # Retorna lista de Rows/Tuplas


def save_daily_aggregates(db: Session, aggregates: List[Tuple], target_date_utc: date):
    """
    Guarda los registros agregados por día en la tabla 'consumo_dia'.
    'aggregates' es la lista de tuplas retornada por aggregate_daily_consumption_from_hourly.
    """
    db_aggregates = []
    now_utc = datetime.now(timezone.utc)
    for agg_row in aggregates:
        total_daily = agg_row.total_daily_kwh
        hour_count = agg_row.hour_count
        # Calcular promedio horario aquí si es necesario
        avg_hourly = total_daily / hour_count if hour_count > 0 else 0

        db_agg = energy_models.ConsumoDia(
            id_sede_fk=agg_row.id_sede_fk,
            fecha_utc=target_date_utc,  # Guardar solo la fecha
            consumo_total_diario_kwh=total_daily,
            numero_horas_registradas=hour_count,
            consumo_promedio_horario_kwh=avg_hourly,
            fecha_procesamiento_utc=now_utc,
        )
        db_aggregates.append(db_agg)

    if db_aggregates:
        try:
            db.add_all(db_aggregates)
            # Commit manejado por el servicio
            print(
                f"  - Preparados {len(db_aggregates)} registros agregados para el día {target_date_utc.strftime('%Y-%m-%d')}"
            )
        except Exception as e:
            print(f"Error en BD al preparar guardado de agregados diarios: {e}")
            raise e


# --- Funciones de ayuda para catch-up diario ---


def find_last_processed_day(db: Session) -> Optional[date]:
    """Encuentra la fecha UTC del último día procesado en ConsumoDia."""
    stmt = select(func.max(energy_models.ConsumoDia.fecha_utc))
    result = db.execute(stmt)
    last_day = result.scalar_one_or_none()
    return last_day


def find_last_date_in_hourly(db: Session) -> Optional[date]:
    """Encuentra la fecha UTC del último registro disponible en ConsumoHora."""
    # Necesitamos la fecha del timestamp, no el timestamp completo
    stmt = select(func.max(cast(energy_models.ConsumoHora.hora_inicio_utc, SQLDate)))
    result = db.execute(stmt)
    last_date = result.scalar_one_or_none()
    return last_date


# --- Función para Consultar Datos Diarios (para el Frontend) ---
def get_daily_consumption(
    db: Session,
    start_date: date,
    end_date: date,
    id_localidad: Optional[str] = None,
    id_sede: Optional[int] = None,
) -> List[energy_models.ConsumoDia]:
    """
    Consulta datos agregados por día dentro de un rango de fechas, con filtros.
    """
    stmt = (
        select(energy_models.ConsumoDia)
        .options(
            # joinedload(energy_models.ConsumoDia.sede) # Opcional si necesitas info de sede
        )
        .where(
            energy_models.ConsumoDia.fecha_utc >= start_date,
            energy_models.ConsumoDia.fecha_utc <= end_date,  # Incluir la fecha de fin
        )
        .order_by(
            energy_models.ConsumoDia.id_sede_fk, energy_models.ConsumoDia.fecha_utc
        )
    )

    if id_sede:
        stmt = stmt.where(energy_models.ConsumoDia.id_sede_fk == id_sede)

    if id_localidad:
        stmt = stmt.join(
            energy_models.Sede,
            energy_models.ConsumoDia.id_sede_fk == energy_models.Sede.id,
        ).where(energy_models.Sede.id_localidad == id_localidad)

    result = db.execute(stmt)
    return result.scalars().all()
