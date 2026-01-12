from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from wsgi import app  # Import app from the wsgi file

if __name__ == '__main__':
    # use_reloader=False prevents the reload loop
    app.run(debug=False, use_reloader=False, host='0.0.0.0')




