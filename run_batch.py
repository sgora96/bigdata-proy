# run_batch.py (sin cambios)
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.join(os.path.dirname(__file__), "app"))
import argparse

from app.database.database import SessionLocal
from app.services import batch_processing_service


def main():
    parser = argparse.ArgumentParser(description="Ejecutar procesos batch de energía.")
    parser.add_argument(
        "mode",
        choices=["hourly", "catchup", "daily", "daily_catchup"],
        help=(
            "'hourly': Procesa la última hora completa. "
            "'catchup': Procesa todas las horas pendientes. "
            "'daily': Procesa el día anterior completo. "
            "'daily_catchup': Procesa todos los días pendientes."
        ),
    )
    args = parser.parse_args()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando ejecución de batch en modo: {args.mode}")
    db = SessionLocal()
    try:
        if args.mode == 'hourly':
            now_utc = datetime.now(timezone.utc)
            batch_processing_service.process_hourly_batch(db=db, target_hour_utc=now_utc)
        elif args.mode == 'catchup':
            batch_processing_service.run_hourly_catchup(db=db)
        elif args.mode == 'daily':
            # Procesar el día ANTERIOR
            yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days=1)
            batch_processing_service.process_daily_batch(db=db, target_date_utc=yesterday_utc)
        elif args.mode == 'daily_catchup':
            batch_processing_service.run_daily_catchup(db=db)

    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] ERROR GENERAL en la ejecución del batch: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
        print(f"[{datetime.now(timezone.utc).isoformat()}] Finalizada ejecución de batch.")

if __name__ == "__main__":
    main()