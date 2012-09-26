from flask import Flask, request, g
from contextlib import closing
from quantile import quantile
from collections import defaultdict
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

MIN_QUANT = 0.1
MAX_QUANT = 0.9
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
    runs_by_type = {}
    boundaries = {}
    stats = {}
    last = {}

    # first pass, group runs
    for run in runs:
        rtype = run.type()
        if run.end_dt is not None:
            if not rtype in runs_by_type:
                runs_by_type[rtype] = []

            if not rtype in last:
                last[rtype] = run.seconds()

            runs_by_type[rtype].append(run)

    # calculate boundary quantiles and most recent run
    for rtype, runs in runs_by_type.items():
        sec_gen = (r.seconds() for r in runs if r.end_dt is not None)
        sorted_secs = sorted(sec_gen)
        boundaries[rtype] = {
            'min': quantile(sorted_secs, MIN_QUANT, 7, True),
            'max': quantile(sorted_secs, MAX_QUANT, 7, True),
        }

    # calculate stats using runs within the boundaries
    for rtype, runs in runs_by_type.items():
        nruns = len(runs)
        stats[rtype] = {'count': 0, 'secs': 0, 'total': nruns}
        for run in runs:
            secs = run.seconds()
            if run.end_dt is not None:
                if nruns < MIN_RUNS or not is_outlier(secs, boundaries[rtype]):
                    stats[rtype]['count'] += 1
                    stats[rtype]['secs'] += secs

    output = {}
    for rtype, stats in stats.items():
        if stats['count'] > 0:
            avg = stats['secs'] / stats['count']
            count = stats['total']
            output[rtype] = {
                'avg': avg,
                'count': count,
                'last': last[rtype]
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
    if rtype in stats:
        count, avg, last = (stats[rtype][k] for k in ['count', 'avg', 'last'])
        return '%s run: %d | Average: %ds | Previous: %ds' % (
            COMMON_RUNS.get(rtype, rtype), count, avg, last)
    else:
        return '%s: first run' % COMMON_RUNS.get(rtype, rtype)

######################################################################
# DB classes
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

    def __repr__(self):
        return '<%s run>' % self.type()

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

def get_everybodys_runs():
    with closing(connect_db()) as db:
        q_cols = ', '.join('r.%s' % x for x in RUN_COLS.split(', '))
        sql = '''select u.username, %s
                 from runs r,
                      run_groups g,
                      users u
                 where u.id = g.user_id
                 and   g.id = r.group_id
              ''' % q_cols
        cursor = db.cursor()
        cursor.execute(sql)
        users = defaultdict(lambda: [])
        for row in cursor:
            users[row[0]].append(Run(*row[1:]))
        return users


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

def is_outlier(x, boundaries):
    return x < boundaries['min'] or x > boundaries['max']

######################################################################
# weekly stats stuff
def leaderboard(stats):
    top = defaultdict(lambda: [])
    for user in stats.keys():
        types = defaultdict(lambda: [])
        for r in stats[user]:
            rtype = r.type()
            types[rtype].append(r)
        for rtype in types.keys():
            all_secs = sorted(x.seconds() for x in types[rtype])
            count = len(all_secs)
            considered = filter(lambda x: x is not None, all_secs)
            nconsidered = len(considered)
            if nconsidered > 0:
                x1 = int(0.1 * nconsidered)
                x2 = int(0.9 * nconsidered)
                total = sum(considered[x1:x2])
                top[rtype].append((count, total/nconsidered, user))

    for rtype in top.keys():
        top[rtype].sort(reverse=True)

    return top

if __name__ == "__main__":
    app.debug = DEBUG
    app.run()
