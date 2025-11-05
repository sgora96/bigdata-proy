# app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import consumption_router, energy_data_router, sede_router
from app.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Código a ejecutar ANTES de que la app empiece a recibir requests
    print("Iniciando aplicación...")
    start_scheduler()  # Iniciar el planificador
    yield  # La aplicación se ejecuta aquí
    # Código a ejecutar DESPUÉS de que la app termine
    print("Apagando aplicación...")
    stop_scheduler()  # Detener el planificador limpiamente


app = FastAPI(
    title="API Consumo Energético",
    description="API para recibir, procesar y exponer datos simulados de consumo energético de colegios de Suba.",
    version="0.1.0",
    lifespan=lifespan,
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",  # Angular en desarrollo
        "http://localhost:3000",  # Por si usas otro puerto
        # Agrega aquí tu dominio de producción cuando lo despliegues
    ],
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos HTTP
    allow_headers=["*"],  # Permite todos los headers
)

app.include_router(
    energy_data_router.router, prefix="/api/energy_data", tags=["Ingesta de datos"]
)

app.include_router(
    consumption_router.router,
    prefix="/api/consumo",
    tags=["Consulta de datos"],
)

app.include_router(
    sede_router.router,
    prefix="/api/sedes",
    tags=["Sedes"]
)


@app.get("/", tags=["Health Check"])
def read_root():
    return {"Estado": "API de Consumo Energético funcionando!"}