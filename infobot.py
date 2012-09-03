from flask import Flask, request, g
from contextlib import closing
import sqlite3
import datetime
import re
import pprint
import os

# configuration
BASE_DIR = os.path.dirname(__file__)
DATABASE = os.path.join(BASE_DIR, 'infobot.db')
SCHEMA_FILE = os.path.join(BASE_DIR, 'schema.sql')
LOG_FILE = os.path.join(BASE_DIR, 'infobot.log')
DEBUG = True

# application
app = Flask(__name__)
app.config.from_object(__name__)

RUN_TYPES = ['meph', 'trav', 'baal', 'mf', 'chaos', 'pind', 'count']
RUN_TYPES_PAT = re.compile('(%s)' % '|'.join(RUN_TYPES))

RUN_COL_NAMES = ['id', 'group_id', 'run_type', 'gamename', 'start_dt', 'end_dt']
RUN_COLS = ', '.join(RUN_COL_NAMES)

######################################################################
# "middleware"
@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    g.db.close()


######################################################################
# routing
@app.route('/', methods=['POST', 'GET'])
def route():
    print 'route'
    if request.method == 'POST':
        log(dict(request.form))
        return post()
    else:
        return home()

@app.route('/<charname>/', methods=['GET'])
def stats(charname):
    return get_stats(charname)

@app.route('/requests/', methods=['GET'])
def print_log():
    with open(LOG_FILE, 'r') as f:
        lines = f.readlines()[-10:]
        return '<br/>\n'.join(lines)


######################################################################
# views
def post():
    message = request.form['Message']

    charname, status, gamename = parse_message(message)
    char_id = get_char(charname)

    if status == 'entered':
        if gamename is None:
            print request.form
        rtype = run_type(gamename)
        group_id = get_group(char_id)
        start_run(group_id, rtype, gamename)
    elif status == 'left':
        stop_run(char_id)

    return ''

def home():
    return 'slashdiablo stats service'

def get_stats(charname):
    char_id = get_char(charname)
    runs = get_all_runs(char_id)
    types = {}
    for run in runs:
        if run.end_dt is not None:
            rtype = run.run_type
            if not rtype in types:
                types[rtype] = {
                    'count': 0,
                    'total_sec': 0
                }
            types[rtype]['count'] += 1
            types[rtype]['total_sec'] += run.seconds()

    for rtype in types.keys():
        types[rtype]['avg'] = types[rtype]['total_sec'] / types[rtype]['count']

    return '<pre>%s</pre>' % pprint.pformat(types)

######################################################################
# message parsing
MSG_PAT = re.compile('^Watched user ([^ ]+) has (left|entered) (.+)')
def parse_message(msg):
    m = MSG_PAT.match(msg)
    if m:
        charname, status, submsg = m.groups()
        gamename = parse_gamename(submsg)
        return (charname, status, gamename)

SUBMSG_PAT = re.compile('^a Diablo II [^"]+"([^"]+)"')
def parse_gamename(submsg):
    m = SUBMSG_PAT.match(submsg)
    if m:
        gamename = m.groups()[0]
        return gamename


######################################################################
# db
def connect_db():
    return sqlite3.connect(app.config['DATABASE'])

def init_db():
    with closing(connect_db()) as db:
        with app.open_resource(SCHEMA_FILE) as f:
            db.cursor().executescript(f.read())
        db.commit()

# chars
def add_char(charname):
    with closing(connect_db()) as db:
        sql = 'insert into chars (charname) values (?)'
        cursor = db.cursor()
        cursor.execute(sql, (charname,))
        db.commit()
        return cursor.lastrowid

def get_char(charname):
    with closing(connect_db()) as db:
        sql = 'select id from chars where charname=?'
        cursor = db.cursor()
        cursor.execute(sql, (charname,))
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            return add_char(charname)

# groups
def add_group(char_id):
    with closing(connect_db()) as db:
        sql = 'insert into run_groups (char_id) values (?)'
        cursor = db.cursor()
        cursor.execute(sql, (char_id,))
        db.commit()
        return cursor.lastrowid

def get_group(char_id):
    with closing(connect_db()) as db:
        sql = 'select id from run_groups where char_id=? order by id desc'
        cursor = db.cursor()
        cursor.execute(sql, (char_id,))
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            return add_group(char_id)

# runs
class Run(object):
    def __init__(self, id, group_id, run_type, gamename, start_dt, end_dt):
        self.id = id
        self.group_id = group_id
        self.run_type = run_type
        self.gamename = gamename
        self.start_dt = mkdt(start_dt)
        self.end_dt = mkdt(end_dt)

    def to_dict(self):
        return dict((col, getattr(self, col)) for col in RUN_COL_NAMES)

    def seconds(self):
        if self.end_dt:
            c = self.end_dt - self.start_dt
            return abs(c.days * 86400 + c.seconds)

def start_run(group_id, run_type, gamename):
    with closing(connect_db()) as db:
        sql = '''insert
                 into runs (group_id, run_type, gamename, start_dt)
                 values (?, ?, ?, ?)'''
        cursor = db.cursor()
        cursor.execute(sql, (group_id, run_type, gamename, now()))
        db.commit()
        return cursor.lastrowid

def stop_run(group_id):
    with closing(connect_db()) as db:
        run = get_run(group_id)
        if run is not None:
            sql = 'update runs set end_dt=? where id=?'
            cursor = db.cursor()
            cursor.execute(sql, (now(), run.id))
            db.commit()
        else:
            print 'run is None'

def get_run(group_id):
    with closing(connect_db()) as db:
        sql = '''select %s
                 from runs
                 where group_id=?
                 order by start_dt desc''' % RUN_COLS
        cursor = db.cursor()
        cursor.execute(sql, (group_id,))
        result = cursor.fetchone()
        if result is not None:
            return Run(*result)
        else:
            return None

def get_all_runs(char_id):
    with closing(connect_db()) as db:
        sql = '''select %s
                 from runs
                 where group_id in (
                     select id from run_groups
                     where char_id = ?
                 )
                 order by start_dt desc''' % RUN_COLS
        cursor = db.cursor()
        cursor.execute(sql, (char_id,))
        return map(lambda row: Run(*row), cursor)


######################################################################
# utils
def now():
    return datetime.datetime.now()

def mkdt(dtstr):
    if dtstr:
        return datetime.datetime.strptime(dtstr.split('.')[0], '%Y-%m-%d %H:%M:%S')

def run_type(gamename):
    m = RUN_TYPES_PAT.search(gamename.lower())
    if m:
        return m.groups()[0]
    return gamename

def log(*args):
    try:
        with open(LOG_FILE, 'a') as f:
            f.write('%s\n' % ' '.join(map(str, args)))
    except:
        pass


if __name__ == "__main__":
    app.debug = DEBUG
    app.run()
