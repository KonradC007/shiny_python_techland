from flask import Flask, render_template_string
from app import app

server = Flask(__name__)

@server.route('/')
def index():
    return render_template_string(open('app.py').read())

if __name__ == '__main__':
    server.run(port=8000, debug=True)