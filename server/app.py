from flask import Flask, render_template, request, jsonify
import os
from testi.index import Index
import time
from urllib.parse import urlparse
from flask_sqlalchemy import SQLAlchemy


INDEX = Index("myindex")

import os

basedir = os.path.dirname(os.path.abspath(__file__))
base_dir  = os.path.split(basedir)[0]

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///{}/queries.db'.format(base_dir)
app.config['SECRET_KEY'] = "vksjdbvbsvw2838eywf8yw8ebgwyiuge98weug8vhbweh"


db = SQLAlchemy(app)

class Query(db.Model):
    id = db.Column('query_id', db.Integer, primary_key = True)
    user_query = db.Column(db.Text)
    ip_addr = db.Column(db.Text)

    def __init__(self, user_query, ip_addr):
        self.user_query = user_query
        self.ip_addr = ip_addr
        
    def save(self):
        db.session.add(self)
        db.session.commit()

def url_fmt(url):
    url = urlparse(url)
    paths = [url.netloc]
    paths += url.path.split("/")
    
    return " › ".join([x for x in paths if x])

def time_fmt(time):
    out = ""
    for x in str(time):
        out += x
        if x not in ["0","."]:
            break
    
    return out

try:
    app.jinja_env.globals.update(__builtins__.__dict__)
    app.jinja_env.globals.update(locals())
except Exception:
    app.jinja_env.globals.update(__builtins__)
    app.jinja_env.globals.update(locals())

basedir = os.path.dirname(os.path.abspath(__file__))
temp_dir = os.path.join(basedir, "templates")

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/search")
def search():
    query = request.args.get("q")
    page  = request.args.get("p",0)
    try:
        page = int(page)
        if page < 1:
            page = 0
        else:
            page = page - 1
    except Exception:
        page = 0
    results = []
    
    if not query:
        query = "news"    
    else:
        q = Query(query, str(request.remote_addr))
        q.save()

    t1 = time.time()
    results = INDEX.search(query.lower().strip(), offset=page)
    t2 = time.time()
    results.time = time_fmt(("%.17f" % (t2-t1)).rstrip('0').rstrip('.')) 

    return render_template("search.html", results=results)

@app.route("/sug")
def suggest():
    query = request.args.get("q")
    suggestions = []
    
    if query:
        results = INDEX.search('title:"{}"'.format(query))
        suggestions = [x.get("highlight") for x in results]
        suggestions = [x for x in suggestions if x.strip()]
        
    return jsonify(suggestions[:5])

@app.route("/queriesss")
def get_queries():
    queries = Query.query.all()
    out = ""
    out += str(len(queries)) + "\n"
    out += "\n | ".join((x.user_query for x in queries))
    return out

#if __name__ == "__main__":
#db.create_all()
