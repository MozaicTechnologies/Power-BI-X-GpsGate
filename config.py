# import os


# class Config:
#     SECRET_KEY = "your-secret-key"
#     basedir = os.path.abspath(os.path.dirname(__file__))
#     db_path = os.path.join(basedir, 'instance', 'render.db')

#     SQLALCHEMY_DATABASE_URI = f"sqlite:///{db_path}"
#     SQLALCHEMY_TRACK_MODIFICATIONS = False

import os

class Config:
    SECRET_KEY = "jjkhjhkjvkvjhvjfjcjvhkhgcfjgccg"

    # Use writable directory on Render
    db_path = '/tmp/render.db'  # ✅ Render allows writing only in /tmp

    SQLALCHEMY_DATABASE_URI = f"sqlite:///{db_path}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
