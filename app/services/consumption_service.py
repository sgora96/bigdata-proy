# app/services/consumption_service.py
from datetime import datetime
from typing import List, Optional, Union

from sqlalchemy.orm import Session

from app.models import energy_models
from app.repositories import energy_repository, sede_repository
from app.schemas import energy_schemas


def get_aggregated_data(
    db: Session,
    start_time: datetime,
    end_time: datetime,
    granularity: str,
    id_sede: Optional[int] = None,
    id_localidad: Optional[str] = None,
) -> List[
    Union[energy_schemas.ConsumoHoraResponseSchema]
]:  # Ajustar Union cuando haya diario
    # ) -> List[Union[energy_schemas.ConsumoHoraResponseSchema, energy_schemas.ConsumoDiaResponseSchema]]: # Así sería con diario
    """
    Obtiene datos agregados según la granularidad y filtros especificados.
    """
    if granularity == "hourly":
        # Llama a la función del repositorio para datos horarios
        hourly_data = energy_repository.get_hourly_consumption(
            db=db,
            start_time=start_time,
            end_time=end_time,
            id_localidad=id_localidad,
            id_sede=id_sede,
        )
        # Convertir los objetos SQLAlchemy a Schemas Pydantic para la respuesta
        # (Si el response_model está bien definido en el router y el schema tiene orm_mode=True,
        # FastAPI puede hacer esto automáticamente, pero hacerlo explícito es más claro)
        # response = [energy_schemas.ConsumoHoraResponseSchema.from_orm(item) for item in hourly_data]
        # return response
        # Por ahora, devolvemos directamente los objetos ORM, FastAPI los manejará
        return hourly_data

    elif granularity == "daily":
        # --- LÓGICA PARA DATOS DIARIOS (A IMPLEMENTAR) ---
        # 1. Llamar a una función similar en el repositorio: energy_repository.get_daily_consumption(...)
        # 2. Esa función consultaría la tabla 'consumo_dia' (que aún no existe)
        # 3. Convertir resultados al schema 'ConsumoDiaResponseSchema' (que aún no existe)
        # 4. Devolver la lista de schemas diarios
        print(
            "WARN: Consulta de granularidad diaria solicitada pero aún no implementada."
        )
        # Por ahora, lanzamos un error específico que el router puede capturar
        raise NotImplementedError(
            "La consulta con granularidad 'daily' aún no está implementada."
        )
        # O podrías retornar una lista vacía: return []

    else:
        # Si la granularidad no es ni 'hourly' ni 'daily'
        raise ValueError(
            f"Granularidad no soportada: {granularity}. Usar 'hourly' o 'daily'."
        )