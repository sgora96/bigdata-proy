# app/routers/energy_data_router.py
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database.database import get_db
from app.schemas import energy_schemas
from app.services import energy_service

router = APIRouter()


@router.post(
    "/",  # La ruta base se definirá en main.py
    status_code=status.HTTP_201_CREATED,
    summary="Recibir lista de lecturas de energía",
    description="Endpoint para recibir un array JSON con múltiples lecturas "
    + "de energía simuladas y guardarlas como datos crudos.",
    tags=["Ingesta de datos"],
)
def recibir_lecturas(
    batch: List[energy_schemas.EnergyReadingCreateSchema],
    db: Session = Depends(get_db),
):
    """
    Recibe una lista de lecturas de energía:
    - **batch**: Array JSON de objetos `EnergyReadingCreateSchema`.
    """
    try:
        num_registros = energy_service.save_readings_batch(db=db, readings=batch)
        if num_registros == len(batch):
            return {
                "mensaje": f"{num_registros} lecturas recibidas y guardadas correctamente."
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Se intentaron guardar {len(batch)} lecturas, "
                + "pero ocurrió un error en el proceso. "
                + f"Registros guardados: {num_registros}",
            )
    except Exception as e:
        print(f"Error HTTP 500 no controlado en endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor al procesar el lote: {str(e)}",
        )
