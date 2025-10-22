# app/routers/consumption_router.py
from datetime import datetime
from typing import List, Optional, Union  # Añadir Union

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database.database import get_db
from app.schemas import energy_schemas  # Importar nuestros schemas
from app.services import consumption_service  # Importaremos el nuevo servicio

# Crear el router
router = APIRouter()


@router.get(
    "/aggregated",
    # Usaremos Union para indicar los posibles tipos de respuesta
    response_model=List[
        energy_schemas.ConsumoHoraResponseSchema
    ],  # Por ahora solo horario
    # response_model=List[Union[energy_schemas.ConsumoHoraResponseSchema, energy_schemas.ConsumoDiaResponseSchema]] # Así sería con diario
    summary="Obtener consumo energético agregado",
    description="Recupera datos de consumo energético agregado por hora o día dentro de un rango de fechas, con filtros opcionales.",
    tags=["Consulta de datos"],
)
def get_aggregated_consumption_data(
    fecha_inicio: datetime = Query(
        ...,
        description="Fecha y hora de inicio del rango (Formato ISO 8601 UTC, ej: 2023-01-01T00:00:00Z)",
    ),
    fecha_fin: datetime = Query(
        ...,
        description="Fecha y hora de fin del rango (Formato ISO 8601 UTC, ej: 2023-01-31T23:59:59Z)",
    ),
    granularidad: str = Query(
        ...,
        description="Granularidad de los datos ('hourly' o 'daily')",
        enum=["hourly", "daily"],
    ),
    id_sede: Optional[int] = Query(
        None, description="Filtrar por un ID de sede específico."
    ),
    id_localidad: Optional[str] = Query(
        None, description="Filtrar por un ID de localidad específico (ej: 'Suba')."
    ),
    db: Session = Depends(get_db),
):
    """
    Endpoint para consultar datos agregados de consumo energético.

    - **fecha_inicio**: Timestamp UTC de inicio.
    - **fecha_fin**: Timestamp UTC de fin.
    - **granularidad**: 'hourly' para datos por hora, 'daily' para datos por día (a implementar).
    - **id_sede**: (Opcional) Filtra por una sede específica.
    - **id_localidad**: (Opcional) Filtra por una localidad específica.
    """
    # Validación simple de fechas
    if fecha_fin <= fecha_inicio:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La fecha de fin debe ser posterior a la fecha de inicio.",
        )

    try:
        # Llamar al servicio para obtener los datos
        results = consumption_service.get_aggregated_data(
            db=db,
            start_time=fecha_inicio,
            end_time=fecha_fin,
            granularity=granularidad,
            id_sede=id_sede,
            id_localidad=id_localidad,
        )
        return results

    except NotImplementedError as nie:
        # Capturar el error si se pide granularidad diaria (aún no lista)
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(nie)
        )
    except ValueError as ve:
        # Capturar otros errores de validación del servicio/repo
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        # Error genérico
        # Loggear el error 'e' en un sistema real
        print(f"Error 500 en get_aggregated_consumption_data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocurrió un error al consultar los datos agregados.",
        )
