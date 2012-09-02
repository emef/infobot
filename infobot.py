from flask import Flask, request, g
from contextlib import closing
import sqlite3

# configuration
DATABASE = '/infobot.db'
DEBUG = True

# application
app = Flask(__name__)

######################################################################
# "middleware"
@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    g.db.close()

######################################################################
# views
@app.route("/")
def route():
    return "Hello World!"

######################################################################
# db
def connect_db():
    return sqlite3.connect(app.config['DATABASE'])

def register(user):
    with closing(connect_db()) as db:
        cursor = db.cursor()
        #etc.
        db.commit()

if __name__ == "__main__":
    app.run()
