from app.core.config import settings  # Importamos la config
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Crear el motor SQLAlchemy
# 'pool_pre_ping=True' ayuda a manejar conexiones que pueden haber sido cerradas por la DB
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)

# Crear una fábrica de sesiones configurada
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Crear una clase Base para que nuestros modelos ORM la hereden
Base = declarative_base()

# --- Dependency para la gestión de sesiones por petición ---
def get_db():
    """
    Función de dependencia de FastAPI para obtener una sesión de BD.
    Asegura que la sesión se cierre correctamente después de cada petición.
    """
    db = SessionLocal()
    try:
        yield db # Proporciona la sesión al endpoint
    finally:
        db.close() # Cierra la sesión al finalizar