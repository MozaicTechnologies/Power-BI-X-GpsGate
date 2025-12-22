from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from wsgi import app  # Import app from the wsgi file

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

