# app/models/energy_models.py

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database.database import Base


class Sede(Base):
    __tablename__ = "sedes"
    id = Column(BigInteger, primary_key=True, index=True, comment="ID único de la sede")
    nombre_sede = Column(
        String, nullable=False, comment="Nombre fusionado o descriptivo"
    )
    id_localidad = Column(String, nullable=False, index=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    fecha_creacion_utc = Column(DateTime(timezone=True), server_default=func.now())
    fecha_actualizacion_utc = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# Lectura recibida por sensor
class LecturaEnergia(Base):
    __tablename__ = "lecturas"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    id_sede_fk = Column(BigInteger, ForeignKey("sedes.id"), nullable=False, index=True)
    timestamp_utc = Column(DateTime(timezone=True), nullable=False, index=True)
    consumo_kwh = Column(Float, nullable=False)
    fecha_recepcion_utc = Column(DateTime(timezone=True), server_default=func.now())
    # Indica si la lectura ha sido procesada y agregada a la tabla de consumo_hora
    procesado = Column(Boolean, default=False, nullable=False, index=True)


# Tabla de agregación por hora
# Esta tabla se llena con un proceso batch que agrega las lecturas por hora
class ConsumoHora(Base):
    __tablename__ = "consumo_hora"  # Nombre de la tabla

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    # Clave Foránea a la tabla 'sedes'
    id_sede_fk = Column(BigInteger, ForeignKey("sedes.id"), nullable=False, index=True)
    # Timestamp de INICIO de la hora de agregación
    hora_inicio_utc = Column(DateTime(timezone=True), nullable=False, index=True)
    # Consumo total calculado para esa hora
    consumo_total_kwh = Column(Float, nullable=False)
    # Número de lecturas crudas que se usaron para calcular este agregado
    numero_lecturas = Column(Integer, nullable=False)
    # Consumo promedio (total / número de lecturas)
    consumo_promedio_kwh = Column(Float, nullable=True)
    # Timestamp de cuándo se realizó este procesamiento batch
    fecha_procesamiento_utc = Column(DateTime(timezone=True), server_default=func.now())

    # Relación con Sede (opcional pero útil)
    sede = relationship("Sede")  # Carga la info de la sede asociada


# Tabla de agregación por día
class ConsumoDia(Base):
    __tablename__ = "consumo_dia"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    # Clave Foránea a la tabla 'sedes'
    id_sede_fk = Column(BigInteger, ForeignKey("sedes.id"), nullable=False, index=True)
    # La FECHA específica de agregación (sin hora)
    fecha_utc = Column(Date, nullable=False, index=True)
    # Consumo total calculado para ese día (suma de los totales horarios)
    consumo_total_diario_kwh = Column(Float, nullable=False)
    # Número de horas que tuvieron registros en consumo_hora para ese día
    numero_horas_registradas = Column(Integer, nullable=False)
    # Consumo promedio por hora (total_diario / horas_registradas)
    consumo_promedio_horario_kwh = Column(Float, nullable=True)
    # Timestamp de cuándo se realizó este procesamiento batch diario
    fecha_procesamiento_utc = Column(DateTime(timezone=True), server_default=func.now())

    # Relación con Sede
    sede = relationship("Sede")

    # UniqueConstraint('id_sede_fk', 'fecha_utc', name='uq_consumo_dia_sede_fecha') # Recomendable
