# app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import consumption_router, energy_data_router, sede_router
from app.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Iniciando aplicación...")
    start_scheduler()
    yield
    print("Apagando aplicación...")
    stop_scheduler()


app = FastAPI(
    title="API Consumo Energético",
    description="API para recibir, procesar y exponer datos simulados de consumo energético de colegios de Suba.",
    version="0.1.0",
    lifespan=lifespan,
    redirect_slashes=False  # ← AGREGAR ESTA LÍNEA
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://localhost:3000",
        "https://tu-dominio-frontend.com",  # Agrega tu dominio de producción
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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