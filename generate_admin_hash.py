"""
Run once to generate the password hash for your admin user.
Copy the output to ADMIN_PASSWORD_HASH in your .env file.

Usage:
    python generate_admin_hash.py
"""
from werkzeug.security import generate_password_hash
import getpass

password = getpass.getpass("Enter admin password: ")
confirm = getpass.getpass("Confirm password: ")

if password != confirm:
    print("Passwords do not match.")
else:
    hashed = generate_password_hash(password)
    print(f"\nAdd this to your .env file:\n\nADMIN_PASSWORD_HASH={hashed}\n")
