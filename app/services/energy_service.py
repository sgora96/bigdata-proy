# app/services/energy_service.py
from typing import List, Tuple

from app.repositories import energy_repository, sede_repository
from app.schemas import energy_schemas
from sqlalchemy.orm import Session


def save_readings_batch(
    db: Session, readings: List[energy_schemas.EnergyReadingCreateSchema]
) -> int:
    """
    Guarda un lote de lecturas de energía, asegurando que las sedes existan ("Get or Create").
    Retorna el número de lecturas guardadas.
    """
    if not readings:
        return 0

    # 1. Extraer IDs de sensor únicos del lote
    incoming_sede_ids = {r.ID_Sensor for r in readings}

    # 2. Consultar cuáles de estas sedes ya existen en la BD
    existing_sedes_dict = sede_repository.get_sedes_by_ids(db, list(incoming_sede_ids))
    existing_sede_ids = set(existing_sedes_dict.keys())

    # 3. Identificar IDs de sedes nuevas
    new_sede_ids = incoming_sede_ids - existing_sede_ids

    # 4. Preparar datos para crear las sedes nuevas (si las hay)
    sedes_to_create_data = []
    if new_sede_ids:
        # Necesitamos encontrar los datos completos de las sedes nuevas en el lote
        processed_new_ids = set()
        for reading in readings:
            if (
                reading.ID_Sensor in new_sede_ids
                and reading.ID_Sensor not in processed_new_ids
            ):
                sede_data = energy_schemas.SedeCreateSchema(
                    id=reading.ID_Sensor,  # Usamos el ID_Sensor como PK de Sede
                    nombre_sede=reading.Nombre_Sede,
                    id_localidad=reading.ID_Localidad,
                    lat=reading.lat,
                    lon=reading.lon,
                )
                sedes_to_create_data.append(sede_data)
                processed_new_ids.add(reading.ID_Sensor)

    # 5. Intentar crear las sedes nuevas y manejar la transacción completa
    created_count = 0
    try:
        # Crear sedes nuevas (si hay)
        if sedes_to_create_data:
            print(f"  - Creando {len(sedes_to_create_data)} nuevas sedes...")
            sede_repository.create_sedes_batch(db, sedes_to_create_data)
            db.flush()  # Escribe los cambios pendientes a la BD sin hacer commit

        # 6. Preparar los datos para insertar las lecturas, asociando la FK correcta
        readings_to_insert_data: List[
            Tuple[energy_schemas.EnergyReadingCreateSchema, int]
        ] = []
        for reading in readings:
            sede_fk_id = reading.ID_Sensor
            readings_to_insert_data.append((reading, sede_fk_id))

        # 7. Insertar el lote de lecturas
        created_count = energy_repository.create_energy_readings_batch(
            db, readings_to_insert_data
        )

        db.commit()
        print(f"  - Commit exitoso. {created_count} lecturas guardadas.")
        return created_count

    except Exception as e:
        print(
            f"Error en la transacción completa (Get or Create Sede + Insert Lecturas): {e}"
        )
        db.rollback()
        return 0
