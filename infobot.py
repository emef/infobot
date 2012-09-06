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

COMMON_RUNS = {
    'meph': 'Mephisto',
    'trav': 'Travincal',
    'baal': 'Baal',
    'mf': 'MF',
    'chaos': 'Chaos',
    'pind': 'Pindleskin',
    'count': 'Countess',
}

COMMON_PAT = re.compile('(%s)' % '|'.join(COMMON_RUNS.keys()))
CUSTOM_PAT = re.compile('([^\d]+)\d')

RUN_COL_NAMES = ['id', 'group_id', 'gamename', 'start_dt', 'end_dt']
RUN_COLS = ', '.join(RUN_COL_NAMES)

OUTLIER = 5.0
MIN_RUNS = 10


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
    if request.method == 'POST':
        log(dict(request.form))
        return post()
    else:
        return home()

@app.route('/<username>/', methods=['GET'])
def stats(username):
    return get_stats(username)

@app.route('/requests/', methods=['GET'])
def print_log():
    with open(LOG_FILE, 'r') as f:
        lines = f.readlines()[-10:]
        return '<br/>\n'.join(lines)


######################################################################
# views
def post():
    message = request.form['Message']

    username, status, gamename = parse_message(message)
    user_id = get_user(username)
    group_id = get_group(user_id)

    if status == 'entered' and gamename is not None:
        start_run(group_id, gamename)
        return start_response(user_id, group_id, gamename)
    elif status == 'left':
        stop_run(group_id)

    return ''

def home():
    return 'slashdiablo stats service'

def get_stats(username):
    user_id = get_user(username)
    runs = get_all_runs(user_id)
    initial = {}
    final = {}
    totals = {}
    output = {}

    # first pass, calc initial average
    for run in runs:
        rtype = run.type()
        if not rtype in initial:
            initial[rtype] = {
                'count': 0,
                'total_sec': 0
            }

        totals[rtype] = totals[rtype] + 1 if (rtype in totals) else 1

        if run.end_dt is not None:
            initial[rtype]['count'] += 1
            initial[rtype]['total_sec'] += run.seconds()

    for rtype in initial.keys():
        initial[rtype]['avg'] = initial[rtype]['total_sec'] / initial[rtype]['count']

    # throw out outliers and recalculate
    for run in runs:
        if run.end_dt is not None:
            rtype = run.type()
            nruns = initial[rtype]['count']
            avg = initial[rtype]['count']

            if not rtype in final:
                final[rtype] = {
                    'count': 0,
                    'total_sec': 0
                }

            if (nruns < MIN_RUNS) or not is_outlier(run.seconds(), avg):
                final[rtype]['count'] += 1
                final[rtype]['total_sec'] += run.seconds()

    for rtype in final.keys():
        avg = final[rtype]['total_sec'] / final[rtype]['count']
        count = totals[rtype]
        output[rtype] = {
            'avg': avg,
            'count': count
        }

    return '<pre>%s</pre>' % pprint.pformat(output)

######################################################################
# message parsing
MSG_PAT = re.compile('^Watched user ([^ ]+) has (left|entered) (.+)')
def parse_message(msg):
    m = MSG_PAT.match(msg)
    if m:
        username, status, submsg = m.groups()
        gamename = parse_gamename(submsg)
        return (username, status, gamename)

SUBMSG_PAT = re.compile('^a Diablo II [^"]+"([^"]+)"')
def parse_gamename(submsg):
    m = SUBMSG_PAT.match(submsg)
    if m:
        gamename = m.groups()[0]
        return gamename

######################################################################
# responses
def start_response(user_id, group_id, gamename):
    rtype = run_type(gamename)
    runs = get_all_runs(user_id)
    count = sum(1 if run.type() == rtype else 0 for run in runs)
    return '%s: %d runs' % (rtype, count)

######################################################################
# db
def connect_db():
    return sqlite3.connect(app.config['DATABASE'])

def init_db():
    with closing(connect_db()) as db:
        with app.open_resource(SCHEMA_FILE) as f:
            db.cursor().executescript(f.read())
        db.commit()

# users
def add_user(username):
    with closing(connect_db()) as db:
        sql = 'insert into users (username) values (?)'
        cursor = db.cursor()
        cursor.execute(sql, (username,))
        db.commit()
        return cursor.lastrowid

def get_user(username):
    with closing(connect_db()) as db:
        sql = 'select id from users where username=?'
        cursor = db.cursor()
        cursor.execute(sql, (username,))
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            return add_user(username)

# groups
def add_group(user_id):
    with closing(connect_db()) as db:
        sql = 'insert into run_groups (user_id) values (?)'
        cursor = db.cursor()
        cursor.execute(sql, (user_id,))
        db.commit()
        return cursor.lastrowid

def get_group(user_id):
    with closing(connect_db()) as db:
        sql = 'select id from run_groups where user_id=? order by id desc'
        cursor = db.cursor()
        cursor.execute(sql, (user_id,))
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            return add_group(user_id)

# runs
class Run(object):
    def __init__(self, id, group_id, gamename, start_dt, end_dt):
        self.id = id
        self.group_id = group_id
        self.gamename = gamename
        self.start_dt = mkdt(start_dt)
        self.end_dt = mkdt(end_dt)

    def to_dict(self):
        return dict((col, getattr(self, col)) for col in RUN_COL_NAMES)

    def seconds(self):
        if self.end_dt:
            c = self.end_dt - self.start_dt
            return abs(c.days * 86400 + c.seconds)

    def type(self):
        return run_type(self.gamename)

def start_run(group_id, gamename):
    with closing(connect_db()) as db:
        sql = '''insert
                 into runs (group_id, gamename, start_dt)
                 values (?, ?, ?)'''
        cursor = db.cursor()
        cursor.execute(sql, (group_id, gamename, now()))
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

def get_all_runs(user_id):
    with closing(connect_db()) as db:
        sql = '''select %s
                 from runs
                 where group_id in (
                     select id from run_groups
                     where user_id = ?
                 )
                 order by start_dt desc''' % RUN_COLS
        cursor = db.cursor()
        cursor.execute(sql, (user_id,))
        return map(lambda row: Run(*row), cursor)


######################################################################
# utils
def now():
    return datetime.datetime.now()

def mkdt(dtstr):
    if dtstr:
        return datetime.datetime.strptime(dtstr.split('.')[0], '%Y-%m-%d %H:%M:%S')

def run_type(gamename):
    # check common run types
    try:
        m = COMMON_PAT.search(gamename.lower())
        if m:
            return COMMON_RUNS[m.groups()[0]]

        # check for custom type
        m = CUSTOM_PAT.match(gamename.lower())
        if m:
            return m.groups()[0]
    except Exception as e:
        log(gamename, str(e))

    return gamename

def log(*args):
    try:
        with open(LOG_FILE, 'a') as f:
            f.write('%s\n' % ' '.join(map(str, args)))
    except:
        pass

def is_outlier(x, avg):
    return x < (avg / OUTLIER) or x > (avg * OUTLIER)





if __name__ == "__main__":
    app.debug = DEBUG
    app.run()
