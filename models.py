from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Render(db.Model):
    __tablename__ = 'render'
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    period_start = db.Column(db.String(50), nullable=False)
    period_end = db.Column(db.String(50), nullable=False)
    tag_id = db.Column(db.String(100), nullable=True)
    event_id = db.Column(db.String(100), nullable=True)
    report_id = db.Column(db.String(100), nullable=False)
    render_id = db.Column(db.String(100), nullable=False, unique=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)



class Result(db.Model):
    __tablename__ = 'result'
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    report_id = db.Column(db.String(100), nullable=False)
    render_id = db.Column(db.String(100), nullable=False, unique=True)
    filepath = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    gdrive_file_id = db.Column(db.String(128), nullable=True)
    gdrive_link    = db.Column(db.String(1024), nullable=True)  # public/direct download link
    uploaded_at    = db.Column(db.DateTime, nullable=True)


class ConsolidatedRequest(db.Model):
    __tablename__ = 'consolidated_requests'
    
    id = db.Column(db.Integer, primary_key=True)
    cache_key = db.Column(db.String(500), unique=True, nullable=False)
    app_id = db.Column(db.String(100), nullable=False)
    report_id = db.Column(db.String(100), nullable=False)
    render_id = db.Column(db.String(100), nullable=False)
    period_start = db.Column(db.String(100))
    period_end = db.Column(db.String(100))
    tag_id = db.Column(db.String(100))
    event_id = db.Column(db.String(100))
    csv_data = db.Column(db.Text)  # Store the cleaned CSV data
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<ConsolidatedRequest {self.cache_key}>'


class FactTrip(db.Model):
    """Trip and Idle fact table for incremental per-week storage"""
    __tablename__ = 'fact_trip'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    start_time = db.Column(db.DateTime, nullable=False)
    stop_time = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.String(50), nullable=True)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    distance_gps = db.Column(db.Float, nullable=True)
    max_speed = db.Column(db.Float, nullable=True)
    avg_speed = db.Column(db.Float, nullable=True)
    event_state = db.Column(db.String(50), nullable=True)  # 'trip' or 'idle'
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    # Unique constraint to prevent duplicates during incremental loading
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'start_time', 'vehicle', name='uq_fact_trip'),
    )
    
    def __repr__(self):
        return f'<FactTrip {self.vehicle} {self.start_time}>'


class FactSpeeding(db.Model):
    """Speeding events fact table"""
    __tablename__ = 'fact_speeding'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    event_time = db.Column(db.DateTime, nullable=False)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    speed = db.Column(db.Float, nullable=True)
    speed_limit = db.Column(db.Float, nullable=True)
    over_limit = db.Column(db.Float, nullable=True)
    duration = db.Column(db.String(50), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_speeding'),
    )


class FactIdle(db.Model):
    """Idle events fact table"""
    __tablename__ = 'fact_idle'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    start_time = db.Column(db.DateTime, nullable=False)
    stop_time = db.Column(db.DateTime, nullable=True)
    duration = db.Column(db.String(50), nullable=True)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'start_time', 'vehicle', name='uq_fact_idle'),
    )


class FactAWH(db.Model):
    """After Work Hours events fact table"""
    __tablename__ = 'fact_awh'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    event_time = db.Column(db.DateTime, nullable=False)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    duration = db.Column(db.String(50), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_awh'),
    )


class FactWH(db.Model):
    """Weekend/Holiday events fact table"""
    __tablename__ = 'fact_wh'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    event_time = db.Column(db.DateTime, nullable=False)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    duration = db.Column(db.String(50), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_wh'),
    )


class FactHA(db.Model):
    """Hard Acceleration events fact table"""
    __tablename__ = 'fact_ha'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    event_time = db.Column(db.DateTime, nullable=False)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    severity = db.Column(db.String(50), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_ha'),
    )


class FactHB(db.Model):
    """Hard Braking events fact table"""
    __tablename__ = 'fact_hb'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    event_time = db.Column(db.DateTime, nullable=False)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    severity = db.Column(db.String(50), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_hb'),
    )


class FactWU(db.Model):
    """Wrong Usage events fact table"""
    __tablename__ = 'fact_wu'
    
    id = db.Column(db.Integer, primary_key=True)
    app_id = db.Column(db.String(100), nullable=False)
    tag_id = db.Column(db.String(100), nullable=False)
    event_date = db.Column(db.DateTime, nullable=False)
    event_time = db.Column(db.DateTime, nullable=False)
    vehicle = db.Column(db.String(255), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    violation_type = db.Column(db.String(100), nullable=True)
    is_duplicate = db.Column(db.Boolean, default=False, nullable=False)  # Flag for duplicate records
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        db.UniqueConstraint('app_id', 'event_date', 'event_time', 'vehicle', name='uq_fact_wu'),
    )