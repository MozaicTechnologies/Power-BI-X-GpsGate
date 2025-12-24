# from flask_sqlalchemy import SQLAlchemy
# from datetime import datetime

# db = SQLAlchemy()

# class BaseEvent(db.Model):
#     __abstract__ = True
#     id = db.Column(db.BigInteger, primary_key=True)
#     app_id = db.Column(db.String(50))
#     tag_id = db.Column(db.String(50))
#     event_date = db.Column(db.Date)
#     created_at = db.Column(db.DateTime, default=datetime.utcnow)
#     __table_args__ = (
#         db.UniqueConstraint(
#             'app_id',
#             'event_date',
#             'start_time',
#             'vehicle',
#             name='uq_event_dedup'
#         ),
#     )

# class FactIdle(BaseEvent):
#     __tablename__ = "fact_idle"

#     start_time = db.Column(db.Time)
#     duration = db.Column(db.String(50))
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))
#     lat = db.Column(db.Float)
#     lon = db.Column(db.Float)

# class FactSpeeding(BaseEvent):
#     __tablename__ = "fact_speeding"

#     start_time = db.Column(db.Time)
#     duration = db.Column(db.String(50))
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))

# class FactAWH(BaseEvent):
#     __tablename__ = "fact_awh"

#     start_time = db.Column(db.Time)
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))


# class FactHA(BaseEvent):
#     __tablename__ = "fact_ha"

#     start_time = db.Column(db.Time)
#     duration = db.Column(db.String(50))
#     duration_s = db.Column(db.Integer)
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))

# class FactHB(BaseEvent):
#     __tablename__ = "fact_hb"

#     start_time = db.Column(db.Time)
#     duration = db.Column(db.String(50))
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))
#     lat = db.Column(db.Float)
#     lon = db.Column(db.Float)

# class FactWH(BaseEvent):
#     __tablename__ = "fact_wh"

#     start_time = db.Column(db.Time)
#     duration_s = db.Column(db.Integer)
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))

# class FactWU(BaseEvent):
#     __tablename__ = "fact_wu"

#     start_time = db.Column(db.Time)
#     duration = db.Column(db.String(50))
#     duration_s = db.Column(db.Integer)
#     vehicle = db.Column(db.String(255))
#     driver = db.Column(db.String(255))

# class FactTrip(BaseEvent):
#     __tablename__ = "fact_trip"

#     start_time = db.Column(db.Time)
#     stop_time = db.Column(db.Time)
#     duration = db.Column(db.String(50))
#     vehicle = db.Column(db.String(255))
#     address = db.Column(db.Text)
#     distance_gps = db.Column(db.Float)
#     max_speed = db.Column(db.Float)
#     avg_speed = db.Column(db.Float)
#     trip_idle_flag = db.Column(db.String(50))



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


# # class ConsolidatedRequest(db.Model):
# #     __tablename__ = 'consolidated_requests'
    
# #     id = db.Column(db.Integer, primary_key=True)
# #     cache_key = db.Column(db.String(500), unique=True, nullable=False)
# #     app_id = db.Column(db.String(100), nullable=False)
# #     report_id = db.Column(db.String(100), nullable=False)
# #     render_id = db.Column(db.String(100), nullable=False)
# #     period_start = db.Column(db.String(100))
# #     period_end = db.Column(db.String(100))
# #     tag_id = db.Column(db.String(100))
# #     event_id = db.Column(db.String(100))
# #     csv_data = db.Column(db.Text)  # Store the cleaned CSV data
# #     created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
#     def __repr__(self):
#         return f'<ConsolidatedRequest {self.cache_key}>'

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy.ext.declarative import declared_attr

db = SQLAlchemy()

# ============================================================================
# ABSTRACT BASE MODEL
# ============================================================================

from sqlalchemy.ext.declarative import declared_attr

class BaseEvent(db.Model):
    __abstract__ = True
    id = db.Column(db.BigInteger, primary_key=True)
    app_id = db.Column(db.String(50), index=True)
    tag_id = db.Column(db.String(50))
    event_date = db.Column(db.Date, index=True) # Start Date
    start_time = db.Column(db.Time)            # Start Time
    duration = db.Column(db.String(50))        # Duration
    vehicle = db.Column(db.String(255), index=True)
    driver = db.Column(db.String(255))         # Driver Name
    location = db.Column(db.Text)              # Start Address / End Address
    event_state = db.Column(db.String(50))     # Event State (from your JSON)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    @declared_attr
    def __table_args__(cls):
        return (db.UniqueConstraint('app_id', 'event_date', 'start_time', 'vehicle', name=f'uq_{cls.__tablename__}_dedup'),)
# ============================================================================
# FACT TABLES (Event Specific)
# ============================================================================

# Targets for fnCSVTrip2 (12-column style)
class FactSpeeding(BaseEvent): __tablename__ = "fact_speeding"
class FactIdle(BaseEvent): __tablename__ = "fact_idle"
class FactAWH(BaseEvent): __tablename__ = "fact_awh"
class FactHA(BaseEvent): __tablename__ = "fact_ha"
class FactHB(BaseEvent): __tablename__ = "fact_hb"
class FactWH(BaseEvent): __tablename__ = "fact_wh"
class FactWU(BaseEvent): __tablename__ = "fact_wu"

# Target for fnCSVTrip (9-column style)
class FactTrip(BaseEvent):
    __tablename__ = "fact_trip"
    stop_time = db.Column(db.Time)        # End Time
    distance_gps = db.Column(db.Float)    # Distance (GPS)
    max_speed = db.Column(db.Float)       # Max Speed
    avg_speed = db.Column(db.Float)       # Avg Speed

# ============================================================================
# SYSTEM TABLES (Tracking API Status)
# ============================================================================

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
    gdrive_link = db.Column(db.String(1024), nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=True)