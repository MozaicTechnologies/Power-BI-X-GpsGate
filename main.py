from dotenv import load_dotenv
load_dotenv()

from wsgi import app

if __name__ == '__main__':
    app.run(
        debug=False,
        use_reloader=False,
        host='0.0.0.0',
        port=5000,         # keep consistent with your pipeline calls
        threaded=True      # ✅ important for long /result polling
    )
