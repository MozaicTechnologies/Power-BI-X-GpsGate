from .models import (
    db,
    Render,
    Result,
    FactTrip,
    FactSpeeding,
    FactIdle,
    FactAWH,
    FactWH,
    FactHA,
    FactHB,
    FactWU,
    CustomerConfig,
    DimTags,
    DimEventRules,
    DimReports,
    DimVehicles,
    DimDrivers,
    DimVehicleCustomFields,
)


def init_db(app):
    """Create all tables that don't exist yet. Safe to call on every startup."""
    import logging
    logger = logging.getLogger(__name__)
    with app.app_context():
        try:
            db.create_all()
            logger.info("[DB] All tables verified / created successfully.")
        except Exception as e:
            logger.error(f"[DB] Failed to initialize database: {e}")
            raise
