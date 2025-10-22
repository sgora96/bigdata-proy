# app/schemas/energy_schemas.py
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


# --- SCHEMAS PARA SEDES ---
# Schema base para Sede
class SedeBaseSchema(BaseModel):
    id: int = Field(..., description="ID único de la sede (original del CSV)")
    nombre_sede: Optional[str] = None
    id_localidad: str
    lat: Optional[float] = None
    lon: Optional[float] = None

# Schema para crear una nueva Sede (idéntico a Base si id es el PK)
class SedeCreateSchema(SedeBaseSchema):
    pass

# Schema para la respuesta de la API al consultar Sedes
class SedeResponseSchema(SedeBaseSchema):
    fecha_creacion_utc: datetime
    fecha_actualizacion_utc: datetime

    class Config:
        orm_mode = True


# --- SCHEMAS PARA LECTURAS DE ENERGÍA ---
# Schema base para Lectura de Energía
class EnergyReadingCreateSchema(BaseModel):
    ID_Sensor: int = Field(..., description="ID de la sede educativa (sensor)")
    Nombre_Sede: Optional[str] = Field(None, description="Nombre de la sede")
    ID_Localidad: str = Field(..., description="Nombre o ID de la localidad", example="Suba")
    TimestampUTC: datetime = Field(..., description="Timestamp de la lectura en formato UTC ISO")
    Consumo_kWh: float = Field(..., ge=0, description="Consumo en kWh para el intervalo")
    lat: Optional[float] = Field(None, description="Latitud")
    lon: Optional[float] = Field(None, description="Longitud")

# Schema para la respuesta de la API (incluye ID y fechas)
class EnergyReadingResponseSchema(EnergyReadingCreateSchema):
    id: int
    fecha_recepcion_utc: datetime
    procesado: bool

    class Config:
        orm_mode = True # Permite crear este schema desde un objeto SQLAlchemy


# --- SCHEMAS PARA CONSUMO AGREGADO POR HORA ---
# Schema base para los datos de consumo horario
class ConsumoHoraBaseSchema(BaseModel):
    id_sede_fk: int
    hora_inicio_utc: datetime
    consumo_total_kwh: float
    numero_lecturas: int
    consumo_promedio_kwh: Optional[float] = None

# Schema para la respuesta de la API (incluye ID y fechas)
class ConsumoHoraResponseSchema(ConsumoHoraBaseSchema):
    id: int
    fecha_procesamiento_utc: datetime
    # Opcional: Incluir info de la Sede anidada si se necesita en la respuesta
    sede: SedeResponseSchema

    class Config:
        orm_mode = True

# --- SCHEMAS PARA CONSUMO AGREGADO POR DÍA ---
# Schema base para datos diarios
class ConsumoDiaBaseSchema(BaseModel):
    id_sede_fk: int
    fecha_utc: date # Usar date para la fecha
    consumo_total_diario_kwh: float
    numero_horas_registradas: int
    consumo_promedio_horario_kwh: Optional[float] = None

# Schema para la respuesta de la API
class ConsumoDiaResponseSchema(ConsumoDiaBaseSchema):
    id: int
    fecha_procesamiento_utc: datetime
    # Opcional: Incluir info de sede anidada si se necesita
    # sede: SedeResponseSchema

    class Config:
        orm_mode = True