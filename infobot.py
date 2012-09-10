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

OUTLIER = 3.0
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
    user_id = get_user(username)
    group_id = get_group(user_id)
    stats = get_stats(group_id)
    return '<pre>%s</pre>' % pprint.pformat(stats)

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
        return start_response(group_id, gamename)
    elif status == 'left':
        stop_run(group_id)

    return ''

def home():
    return 'slashdiablo stats service'

def get_stats(group_id):
    runs = get_group_runs(group_id)
    stats = {}

    # first pass, calc initial average
    for run in runs:
        rtype = run.type()
        if not rtype in stats:
            stats[rtype] = {
                'total': 0,
                'initial_count': 0,
                'initial_sec': 0,
                'final_count': 0,
                'final_sec': 0
            }

        stats[rtype]['total'] += 1

        if run.end_dt is not None:
            stats[rtype]['initial_count'] += 1
            stats[rtype]['initial_sec'] += run.seconds()

    # calculate initial averages
    zero_runs = set()
    for rtype in stats.keys():
        if stats[rtype]['initial_count'] == 0:
            zero_runs.add(rtype)
        else:
            avg = stats[rtype]['initial_sec'] / stats[rtype]['initial_count']
            stats[rtype]['initial_avg'] = avg

    # throw out zero runners
    for rtype in zero_runs:
        del stats[rtype]

    # throw out outliers and recalculate
    for run in runs:
        if run.end_dt is not None:
            rtype = run.type()
            nruns = stats[rtype]['initial_count']
            avg = stats[rtype]['initial_sec'] / nruns

            if (nruns < MIN_RUNS) or not is_outlier(run.seconds(), avg):
                stats[rtype]['final_count'] += 1
                stats[rtype]['final_sec'] += run.seconds()

    output = {}
    for rtype in stats.keys():
        avg = stats[rtype]['final_sec'] / stats[rtype]['final_count']
        count = stats[rtype]['total']
        output[rtype] = {
            'avg': avg,
            'count': count
        }

    return output

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
def start_response(group_id, gamename):
    rtype = run_type(gamename)
    stats = get_stats(group_id)
    count = stats[rtype]['count']
    avg = stats[rtype]['avg']
    return '%s: %d runs (%dsec/run)' % (rtype, count, avg)

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

def get_group_runs(group_id):
    with closing(connect_db()) as db:
        sql = '''select %s
                 from runs
                 where group_id = ?
                 order by start_dt desc''' % RUN_COLS
        cursor = db.cursor()
        cursor.execute(sql, (group_id,))
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
            return m.groups()[0]

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
