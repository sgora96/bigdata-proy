# app/repositories/sede_repository.py
from typing import Dict, List, Optional  # Añadir Dict

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import energy_models
from app.schemas import energy_schemas


def get_sede_by_id(db: Session, sede_id: int) -> Optional[energy_models.Sede]:
    """Busca una sede por su ID."""
    return db.get(energy_models.Sede, sede_id)

def get_sedes_by_ids(db: Session, sede_ids: List[int]) -> Dict[int, energy_models.Sede]:
    """Busca múltiples sedes por sus IDs y retorna un diccionario id -> objeto Sede."""
    if not sede_ids:
        return {}
    stmt = select(energy_models.Sede).where(energy_models.Sede.id.in_(sede_ids))
    result = db.execute(stmt)
    sedes_encontradas = result.scalars().all()
    # Convertir lista a diccionario para acceso rápido por ID
    return {sede.id: sede for sede in sedes_encontradas}

def get_all_sedes(db: Session) -> List[energy_models.Sede]:
    """Obtiene todas las sedes de la base de datos, ordenadas por nombre."""
    stmt = select(energy_models.Sede).order_by(energy_models.Sede.nombre_sede)
    result = db.execute(stmt)
    return result.scalars().all() # Devuelve lista de objetos Sede

def create_sede(db: Session, sede_data: energy_schemas.SedeCreateSchema) -> energy_models.Sede:
    """Crea una nueva sede en la base de datos."""
    db_sede = energy_models.Sede(
        id=sede_data.id, # Asignar el ID recibido
        nombre_sede=sede_data.nombre_sede,
        id_localidad=sede_data.id_localidad,
        lat=sede_data.lat,
        lon=sede_data.lon
    )
    db.add(db_sede)
    return db_sede

def create_sedes_batch(db: Session, sedes_data: List[energy_schemas.SedeCreateSchema]) -> List[energy_models.Sede]:
     """Crea múltiples sedes nuevas."""
     db_sedes = []
     for sede_in in sedes_data:
          db_sede = energy_models.Sede(
               id=sede_in.id,
               nombre_sede=sede_in.nombre_sede,
               id_localidad=sede_in.id_localidad,
               lat=sede_in.lat,
               lon=sede_in.lon
          )
          db_sedes.append(db_sede)

     if db_sedes:
          db.add_all(db_sedes)
     # El commit lo maneja el servicio que llama a esta función
     return db_sedes # Devolver los objetos creados (aún sin commit)