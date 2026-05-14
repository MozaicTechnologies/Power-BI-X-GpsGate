from celery import Celery, Task

celery = Celery("power_bi_gpsgate")


def configure_celery(app):
    """Bind Celery to Flask app so every task runs inside app context."""
    celery.conf.update(app.config.get("CELERY", {}))

    celery.conf.resultrepr_maxsize = int(
    "100000"
)

    class FlaskTask(Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = FlaskTask
    app.extensions["celery"] = celery
    return celery
