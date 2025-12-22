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