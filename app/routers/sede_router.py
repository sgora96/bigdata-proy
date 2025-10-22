# app/routers/sede_router.py
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database.database import get_db
from app.schemas import energy_schemas  # Necesitamos SedeResponseSchema
from app.services import sede_service  # Importar el NUEVO servicio de sedes

# Crear el router específico para sedes
router = APIRouter()

# --- ENDPOINT PARA OBTENER TODAS LAS SEDES ---
@router.get(
    "/", # La ruta base será /api/sedes (definido en main.py)
    response_model=List[energy_schemas.SedeResponseSchema], # Lista de sedes
    summary="Obtener Lista de Todas las Sedes",
    tags=["Sedes"] # Tag específico para agrupar en Swagger
)
def read_all_sedes(db: Session = Depends(get_db)):
    """
    Recupera una lista completa de todas las sedes educativas registradas.
    """
    try:
        sedes = sede_service.get_all_sedes(db=db)
        return sedes # FastAPI maneja la conversión a SedeResponseSchema
    except Exception as e:
        print(f"Error 500 en read_all_sedes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ocurrió un error al obtener la lista de sedes."
        )

# --- ENDPOINT PARA OBTENER DETALLES DE UNA SEDE POR ID ---
@router.get(
    "/{sede_id}", # Ruta relativa: /api/sedes/{id}
    response_model=energy_schemas.SedeResponseSchema, # Devuelve una sola sede
    summary="Obtener Detalles de una Sede Específica",
    tags=["Sedes"],
    responses={404: {"description": "Sede no encontrada"}} # Documentar el 404
)
def read_sede_by_id(sede_id: int, db: Session = Depends(get_db)):
    """
    Recupera los detalles completos de una sede específica utilizando su ID.
    """
    try:
        sede = sede_service.get_sede_details(db=db, sede_id=sede_id)
        if sede is None:
            # Si el servicio devuelve None, lanzar un error 404
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sede con id {sede_id} no encontrada."
            )
        return sede # FastAPI maneja la conversión
    except HTTPException as http_exc:
        # Relanzar excepciones HTTP (como el 404)
        raise http_exc
    except Exception as e:
        print(f"Error 500 en read_sede_by_id para id {sede_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ocurrió un error al obtener detalles de la sede {sede_id}."
        )