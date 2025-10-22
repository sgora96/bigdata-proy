# app/services/sede_service.py
from typing import List, Optional

from sqlalchemy.orm import Session

from app.models import energy_models  # Importar el modelo Sede
from app.repositories import sede_repository  # Importar el repositorio de sedes


def get_all_sedes(db: Session) -> List[energy_models.Sede]:
    """
    Obtiene la lista de todas las sedes disponibles.
    Llama directamente al repositorio.
    """
    return sede_repository.get_all_sedes(db=db)

def get_sede_details(db: Session, sede_id: int) -> Optional[energy_models.Sede]:
    """
    Obtiene los detalles de una sede específica por su ID.
    Llama directamente al repositorio.
    Retorna el objeto Sede o None si no se encuentra.
    """
    sede = sede_repository.get_sede_by_id(db=db, sede_id=sede_id)
    # No es necesario manejar el 'not found' aquí, el repositorio devuelve None
    # El router se encargará de convertir el None en un 404 Not Found.
    return sede