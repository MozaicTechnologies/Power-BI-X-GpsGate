from wsgi import app  # Import app from the wsgi file

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

