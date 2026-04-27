# from flask_sqlalchemy import SQLAlchemy
# from datetime import datetime

# db = SQLAlchemy()

# class Render(db.Model):
#     __tablename__ = 'render'
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     period_start = db.Column(db.String(50), nullable=False)
#     period_end = db.Column(db.String(50), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=True)
#     event_id = db.Column(db.String(100), nullable=True)
#     report_id = db.Column(db.String(100), nullable=False)
#     render_id = db.Column(db.String(100), nullable=False, unique=True)
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)



# class Result(db.Model):
#     __tablename__ = 'result'
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     report_id = db.Column(db.String(100), nullable=False)
#     render_id = db.Column(db.String(100), nullable=False, unique=True)
#     filepath = db.Column(db.String(100), nullable=False)
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
#     gdrive_file_id = db.Column(db.String(128), nullable=True)
#     gdrive_link    = db.Column(db.String(1024), nullable=True)  # public/direct download link
#     uploaded_at    = db.Column(db.DateTime, nullable=True)


# class ConsolidatedRequest(db.Model):
#     __tablename__ = 'consolidated_requests'
    
#     id = db.Column(db.Integer, primary_key=True)
#     cache_key = db.Column(db.String(500), unique=True, nullable=False)
#     app_id = db.Column(db.String(100), nullable=False)
#     report_id = db.Column(db.String(100), nullable=False)
#     render_id = db.Column(db.String(100), nullable=False)
#     period_start = db.Column(db.String(100))
#     period_end = db.Column(db.String(100))
#     tag_id = db.Column(db.String(100))
#     event_id = db.Column(db.String(100))
#     csv_data = db.Column(db.Text)  # Store the cleaned CSV data
#     created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
#     def __repr__(self):
#         return f'<ConsolidatedRequest {self.cache_key}>'


# class FactTrip(db.Model):
#     """Trip and Idle fact table for incremental per-week storage"""
#     __tablename__ = 'fact_trip'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     start_time = db.Column(db.DateTime, nullable=False)
#     stop_time = db.Column(db.DateTime, nullable=True)
#     duration = db.Column(db.String(50), nullable=True)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     distance_gps = db.Column(db.Float, nullable=True)
#     max_speed = db.Column(db.Float, nullable=True)
#     avg_speed = db.Column(db.Float, nullable=True)
#     event_state = db.Column(db.String(50), nullable=True)  # 'trip' or 'idle'
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     # Unique constraint to prevent duplicates during incremental loading
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'start_time', 'vehicle', name='uq_fact_trip'),
#     )
    
#     def __repr__(self):
#         return f'<FactTrip {self.vehicle} {self.start_time}>'


# class FactSpeeding(db.Model):
#     """Speeding events fact table"""
#     __tablename__ = 'fact_speeding'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     event_time = db.Column(db.DateTime, nullable=False)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     speed = db.Column(db.Float, nullable=True)
#     speed_limit = db.Column(db.Float, nullable=True)
#     over_limit = db.Column(db.Float, nullable=True)
#     duration = db.Column(db.String(50), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_speeding'),
#     )


# class FactIdle(db.Model):
#     """Idle events fact table"""
#     __tablename__ = 'fact_idle'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     start_time = db.Column(db.DateTime, nullable=False)
#     stop_time = db.Column(db.DateTime, nullable=True)
#     duration = db.Column(db.String(50), nullable=True)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'start_time', 'vehicle', name='uq_fact_idle'),
#     )


# class FactAWH(db.Model):
#     """After Work Hours events fact table"""
#     __tablename__ = 'fact_awh'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     event_time = db.Column(db.DateTime, nullable=False)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     duration = db.Column(db.String(50), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_awh'),
#     )


# class FactWH(db.Model):
#     """Weekend/Holiday events fact table"""
#     __tablename__ = 'fact_wh'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     event_time = db.Column(db.DateTime, nullable=False)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     duration = db.Column(db.String(50), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_wh'),
#     )


# class FactHA(db.Model):
#     """Hard Acceleration events fact table"""
#     __tablename__ = 'fact_ha'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     event_time = db.Column(db.DateTime, nullable=False)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     severity = db.Column(db.String(50), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_ha'),
#     )


# class FactHB(db.Model):
#     """Hard Braking events fact table"""
#     __tablename__ = 'fact_hb'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     event_time = db.Column(db.DateTime, nullable=False)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     severity = db.Column(db.String(50), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_hb'),
#     )


# class FactWU(db.Model):
#     """Wrong Usage events fact table"""
#     __tablename__ = 'fact_wu'
    
#     id = db.Column(db.Integer, primary_key=True)
#     app_id = db.Column(db.String(100), nullable=False)
#     tag_id = db.Column(db.String(100), nullable=False)
#     event_date = db.Column(db.DateTime, nullable=False)
#     event_time = db.Column(db.DateTime, nullable=False)
#     vehicle = db.Column(db.String(255), nullable=True)
#     location = db.Column(db.String(500), nullable=True)
#     violation_type = db.Column(db.String(100), nullable=True)
#     is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
#     created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
#     __table_args__ = (
#         db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_wu'),
#     )

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

# ------------------------------------------------------------------
# RENDER / RESULT
# ------------------------------------------------------------------

class Render(db.Model):
    __tablename__ = "render"

    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    period_start = db.Column(db.String(50), nullable=False)
    period_end = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(100))
    event_id = db.Column(db.String(100))
    report_id = db.Column(db.String(100), nullable=False)
    render_id = db.Column(db.String(100), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Result(db.Model):
    __tablename__ = "result"

    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    report_id = db.Column(db.String(100), nullable=False)
    render_id = db.Column(db.String(100), unique=True, nullable=False)
    filepath = db.Column(db.String(255))
    gdrive_file_id = db.Column(db.String(128))
    gdrive_link = db.Column(db.String(1024))
    uploaded_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ------------------------------------------------------------------
# FACT TABLES (NO MIXIN – EXPLICIT IS SAFER)
# ------------------------------------------------------------------

class FactTrip(db.Model):
    __tablename__ = "fact_trip"

    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(50), nullable=False)

    event_date = db.Column(db.DateTime, nullable=False)
    start_time = db.Column(db.Time, nullable=False)
    stop_time = db.Column(db.DateTime)

    vehicle = db.Column(db.String(255))
    address = db.Column(db.Text)
    location = db.Column(db.Text)

    duration = db.Column(db.String(50))
    distance_gps = db.Column(db.Float)
    max_speed = db.Column(db.Float)
    avg_speed = db.Column(db.Float)

    event_state = db.Column(db.String(50))
    is_duplicate = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("app_id", "event_date", "start_time", "vehicle",
                            name="uq_fact_trip"),
    )


class FactSpeeding(db.Model):
    __tablename__ = "fact_speeding"

    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(50), nullable=False)

    event_date = db.Column(db.Date, nullable=False)
    start_time = db.Column(db.Time, nullable=False)

    vehicle = db.Column(db.String(255))
    driver = db.Column(db.String(255))

    location = db.Column(db.Text)
    address = db.Column(db.Text)

    duration = db.Column(db.String(50))
    duration_s = db.Column(db.Integer)

    speed = db.Column(db.Float)
    speed_limit = db.Column(db.Float)
    over_limit = db.Column(db.Float)

    is_duplicate = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("app_id", "event_date", "start_time", "vehicle",
                            name="uq_fact_speeding"),
    )


class FactIdle(db.Model):
    __tablename__ = "fact_idle"

    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(50), nullable=False)

    event_date = db.Column(db.Date, nullable=False)
    start_time = db.Column(db.Time, nullable=False)

    vehicle = db.Column(db.String(255))
    driver = db.Column(db.String(255))

    location = db.Column(db.Text)
    address = db.Column(db.Text)

    lat = db.Column(db.Float)
    lon = db.Column(db.Float)

    duration = db.Column(db.String(50))
    duration_s = db.Column(db.Integer)

    is_duplicate = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("app_id", "event_date", "start_time", "vehicle",
                            name="uq_fact_idle"),
    )


class FactAWH(db.Model):
    __tablename__ = "fact_awh"

    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(50), nullable=False)

    event_date = db.Column(db.Date, nullable=False)
    start_time = db.Column(db.Time, nullable=False)

    vehicle = db.Column(db.String(255))
    driver = db.Column(db.String(255))
    location = db.Column(db.Text)
    address = db.Column(db.Text)

    duration = db.Column(db.String(50))
    duration_s = db.Column(db.Integer)

    is_duplicate = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("app_id", "event_date", "start_time", "vehicle",
                            name="uq_fact_awh"),
    )


class FactWH(FactAWH):
    __tablename__ = "fact_wh"


class FactHA(db.Model):
    __tablename__ = "fact_ha"

    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(50), nullable=False)

    event_date = db.Column(db.Date, nullable=False)
    start_time = db.Column(db.Time, nullable=False)

    vehicle = db.Column(db.String(255))
    driver = db.Column(db.String(255))
    severity = db.Column(db.String(50))

    location = db.Column(db.Text)
    address = db.Column(db.Text)

    duration = db.Column(db.String(50))
    duration_s = db.Column(db.Integer)

    is_duplicate = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("app_id", "event_date", "start_time", "vehicle",
                            name="uq_fact_ha"),
    )


class FactHB(FactHA):
    __tablename__ = "fact_hb"


class FactWU(db.Model):
    __tablename__ = "fact_wu"

    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(50), nullable=False)

    event_date = db.Column(db.Date, nullable=False)
    start_time = db.Column(db.Time, nullable=False)

    vehicle = db.Column(db.String(255))
    driver = db.Column(db.String(255))
    violation_type = db.Column(db.String(100))

    location = db.Column(db.Text)
    address = db.Column(db.Text)

    duration = db.Column(db.String(50))
    duration_s = db.Column(db.Integer)

    is_duplicate = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("app_id", "event_date", "start_time", "vehicle",
                            name="uq_fact_wu"),
    )


# ------------------------------------------------------------------
# JOB EXECUTION TRACKING
# ------------------------------------------------------------------

class JobExecution(db.Model):
    """Track scheduled and manual job executions"""
    __tablename__ = "job_execution"

    id = db.Column(db.BigInteger, primary_key=True)
    job_type = db.Column(db.String(100), nullable=False)  # daily_sync, weekly_backfill, manual_dimension_sync, etc.
    status = db.Column(db.String(50), nullable=False)  # running, completed, failed
    
    started_at = db.Column(db.DateTime, nullable=False)
    completed_at = db.Column(db.DateTime)
    
    records_processed = db.Column(db.Integer, default=0)
    errors = db.Column(db.Integer, default=0)
    
    error_message = db.Column(db.Text)
    job_metadata = db.Column(db.JSON)  # Store job-specific data (date ranges, results, etc.)
    
    triggered_by = db.Column(db.String(100))  # 'scheduler', 'manual', 'api'
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<JobExecution {self.job_type} {self.status} {self.started_at}>'
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            'id': self.id,
            'job_type': self.job_type,
            'status': self.status,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'records_processed': self.records_processed,
            'errors': self.errors,
            'error_message': self.error_message,
            'metadata': self.job_metadata,
            'triggered_by': self.triggered_by
        }


class CustomerConfig(db.Model):
    """Per-application customer configuration for dynamic report and event IDs."""
    __tablename__ = "customer_config"

    application_id = db.Column(db.String(50), primary_key=True)
    token = db.Column(db.Text, nullable=False)
    tag_name = db.Column(db.String(255))
    trip_report_name = db.Column(db.String(255))
    event_report_name = db.Column(db.String(255))
    speed_event_rule_name = db.Column(db.String(255))
    idle_event_rule_name = db.Column(db.String(255))
    awh_event_rule_name = db.Column(db.String(255))
    ha_event_rule_name = db.Column(db.String(255))
    hb_event_rule_name = db.Column(db.String(255))
    hc_event_rule_name = db.Column(db.String(255))
    wu_event_rule_name = db.Column(db.String(255))
    wh_event_rule_name = db.Column(db.String(255))
    tag_id = db.Column(db.String(50))

    trip_report_id = db.Column(db.String(50))
    event_report_id = db.Column(db.String(50))

    awh_event_id = db.Column(db.String(50))
    ha_event_id = db.Column(db.String(50))
    hb_event_id = db.Column(db.String(50))
    hc_event_id = db.Column(db.String(50))
    wu_event_id = db.Column(db.String(50))
    wh_event_id = db.Column(db.String(50))
    speed_event_id = db.Column(db.String(50))
    idle_event_id = db.Column(db.String(50))


# Dimension Tables (created by sync_dimensions_from_api.py)

class DimTags(db.Model):
    """Dimension table for tags/groups"""
    __tablename__ = "dim_tags"

    id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    application_id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    name = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class DimEventRules(db.Model):
    """Dimension table for event rules"""
    __tablename__ = "dim_event_rules"

    id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    application_id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    name = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class DimReports(db.Model):
    """Dimension table for reports"""
    __tablename__ = "dim_reports"

    id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    application_id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    name = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class DimVehicles(db.Model):
    """Dimension table for vehicles"""
    __tablename__ = "dim_vehicles"

    id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    application_id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    name = db.Column(db.Text)
    username = db.Column(db.Text)
    driver_id = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    device_name = db.Column(db.Text)
    imei = db.Column(db.Text)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    utc = db.Column(db.DateTime)
    validity = db.Column(db.Boolean)


class DimDrivers(db.Model):
    """Dimension table for drivers"""
    __tablename__ = "dim_drivers"

    id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    application_id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    name = db.Column(db.Text)
    username = db.Column(db.Text)
    driver_id = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    device_name = db.Column(db.Text)
    imei = db.Column(db.Text)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    utc = db.Column(db.DateTime)
    validity = db.Column(db.Boolean)


class DimVehicleCustomFields(db.Model):
    """Dimension table for vehicle custom fields"""
    __tablename__ = "dim_vehicle_custom_fields"

    vehicle_id = db.Column(db.BigInteger, nullable=False, primary_key=True)
    field_name = db.Column(db.Text, nullable=False, primary_key=True)
    field_value = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    application_id = db.Column(db.Integer, nullable=False, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("application_id", "vehicle_id", "field_name", name="uq_dim_vehicle_custom_fields"),
    )
