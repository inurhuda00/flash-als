import os
import pymongo

# Each Flask web application contains a secret key which used to sign session cookies for protection against cookie data tampering.
SECRET_KEY = os.urandom(32)

# Grabs the folder where the script runs.
basedir = os.path.abspath(os.path.dirname(__file__))

# Enable debug mode, that will refresh the page when you make changes.
DEBUG = True
