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
