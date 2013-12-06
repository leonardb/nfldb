from __future__ import absolute_import, division, print_function
import ConfigParser
import datetime
import os
import os.path as path
import re
import sys

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS
from psycopg2.extensions import new_type, register_type

import pytz

import nfldb.team

__pdoc__ = {}

api_version = 5
__pdoc__['api_version'] = \
    """
    The schema version that this library corresponds to. When the schema
    version of the database is less than this value, `nfldb.connect` will
    automatically update the schema to the latest version before doing
    anything else.
    """

_config_home = os.getenv('XDG_CONFIG_HOME')
if not _config_home:
    home = os.getenv('HOME')
    if not home:
        _config_home = ''
    else:
        _config_home = path.join(home, '.config')

def pfx(c, ddl):
    """
    Replace token with specific prefix to allow absolute
    references to resources"""
    if c.schema is not None:
      return ddl.replace('_PFX_', c.schema)
    else:
      return ddl.replace('_PFX_', 'public')

def config(config_path=''):
    """
    Reads and loads the configuration file containing PostgreSQL
    connection information. This function is used automatically
    by `nfldb.connect`.

    The return value is a dictionary mapping a key in the configuration
    file to its corresponding value. All values are strings, except for
    `port`, which is always an integer.

    A total of three possible file paths are tried before
    giving up and returning `None`. The file paths, in
    order, are: `fp`, `sys.prefix/share/nfldb/config.ini` and
    `$XDG_CONFIG_HOME/nfldb/config.ini`.
    """
    paths = [
        config_path,
        path.join(sys.prefix, 'share', 'nfldb', 'config.ini'),
        path.join(_config_home, 'nfldb', 'config.ini'),
    ]
    cp = ConfigParser.RawConfigParser()
    for p in paths:
        try:
            with open(p) as fp:
                cp.readfp(fp)
                return {
                    'timezone': cp.get('pgsql', 'timezone'),
                    'database': cp.get('pgsql', 'database'),
                    'search_path': cp.get('pgsql', 'search_path'),
                    'user': cp.get('pgsql', 'user'),
                    'password': cp.get('pgsql', 'password'),
                    'host': cp.get('pgsql', 'host'),
                    'port': cp.getint('pgsql', 'port'),
                }
        except IOError:
            pass
    return None


def connect(database=None, user=None, password=None, host=None, port=None,
            timezone=None, config_path='', search_path=None):
    """
    Returns a `psycopg2._psycopg.connection` object from the
    `psycopg2.connect` function. If database is `None`, then `connect`
    will look for a configuration file using `nfldb.config` with
    `config_path`. Otherwise, the connection will use the parameters
    given.

    If `database` is `None` and no config file can be found, then an
    `IOError` exception is raised.

    This function will also compare the current schema version of the
    database against the API version `nfldb.api_version` and assert
    that they are equivalent. If the schema library version is less
    than the the API version, then the schema will be automatically
    upgraded. If the schema version is newer than the library version,
    then this function will raise an assertion error. An assertion
    error will also be raised if the schema version is 0 and the
    database is not empty.

    N.B. The `timezone` parameter should be set to a value that
    PostgreSQL will accept. Select from the `pg_timezone_names` view
    to get a list of valid time zones.
    """
    if database is None:
        conf = config(config_path=config_path)
        if conf is None:
            raise IOError("Could not find valid configuration file.")

        timezone, database = conf['timezone'], conf['database']
        user, password = conf['user'], conf['password']
        host, port = conf['host'], conf['port']
        search_path = conf['search_path']

    if search_path is None:
        search_path = 'public'

    conn = (psycopg2.connect(database=database, user=user, password=password,
                            host=host, port=port), search_path)
    # Start the migration. Make sure if this is the initial setup that
    # the DB is empty.
    sversion = schema_version(conn)
    assert sversion <= api_version, \
        'Library with version %d is older than the schema with version %d' \
        % (api_version, sversion)
    assert sversion > 0 or (sversion == 0 and _is_empty(conn)), \
        'Schema has version 0 but is not empty.'
    set_timezone(conn, 'UTC')
    _migrate(conn, api_version)

    if timezone is not None:
        set_timezone(conn, timezone)

    # Bind SQL -> Python casting functions.
    from nfldb.types import Clock, _Enum, Enums, FieldPosition, PossessionTime
    _bind_type(conn, 'game_phase', _Enum._pg_cast(Enums.game_phase))
    _bind_type(conn, 'season_phase', _Enum._pg_cast(Enums.season_phase))
    _bind_type(conn, 'game_day', _Enum._pg_cast(Enums.game_day))
    _bind_type(conn, 'player_pos', _Enum._pg_cast(Enums.player_pos))
    _bind_type(conn, 'player_status', _Enum._pg_cast(Enums.player_status))
    _bind_type(conn, 'game_time', Clock._pg_cast)
    _bind_type(conn, 'pos_period', PossessionTime._pg_cast)
    _bind_type(conn, 'field_pos', FieldPosition._pg_cast)

    return conn


def schema_version(conn):
    """
    Returns the schema version of the given database. If the version
    is not stored in the database, then `0` is returned.
    """
    with Tx(conn) as c:
        try:
            c.execute(pfx(c, 'SELECT version FROM _PFX_.meta LIMIT 1'), ['version'])
        except psycopg2.ProgrammingError:
            conn[0].rollback()
            return 0
        if c.rowcount == 0:
            return 0
        return c.fetchone()['version']


def set_timezone(conn, timezone):
    """
    Sets the timezone for which all datetimes will be displayed
    as. Valid values are exactly the same set of values accepted
    by PostgreSQL. (Select from the `pg_timezone_names` view to
    get a list of valid time zones.)

    Note that all datetimes are stored in UTC. This setting only
    affects how datetimes are viewed from select queries.
    """
    with Tx(conn) as c:
        c.execute('SET timezone = %s', (timezone,))


def now():
    """
    Returns the current date/time in UTC as a `datetime.datetime`
    object. It can be used to compare against date/times in any of the
    `nfldb` objects without worrying about timezones.
    """
    return datetime.datetime.now(pytz.utc)


def _bind_type(conn, sql_type_name, cast):
    """
    Binds a `cast` function to the SQL type in the connection `conn`
    given by `sql_type_name`. `cast` must be a function with two
    parameters: the SQL value and a cursor object. It should return the
    appropriate Python object.

    Note that `sql_type_name` is not escaped.
    """
    with Tx(conn) as c:
        c.execute('SELECT NULL::%s' % sql_type_name)
        typ = new_type((c.description[0].type_code,), sql_type_name, cast)
        register_type(typ)


def _db_name(conn):
    m = re.search('dbname=(\S+)', conn.dsn)
    return m.group(1)


def _is_empty(conn):
    """
    Returns `True` if and only if there are no tables in the given
    database.
    """
    with Tx(conn) as c:
        c.execute(pfx(c, '''
            SELECT COUNT(*) AS count FROM information_schema.tables
            WHERE table_catalog = %s AND table_schema = '_PFX_'
        '''), [_db_name(conn[0])])
        if c.fetchone()['count'] == 0:
            return True
    return False


def _mogrify(cursor, xs):
    """Shortcut for mogrifying a list as if it were a tuple."""
    return cursor.mogrify('%s', (tuple(xs),))


def _num_rows(cursor, table):
    """Returns the number of rows in table."""
    cursor.execute(pfx(cursor, 'SELECT COUNT(*) AS rowcount FROM _PFX_.%s') % table)
    return cursor.fetchone()['rowcount']


class Tx (object):
    """
    Tx is a `with` compatible class that abstracts a transaction given
    a connection. If an exception occurs inside the `with` block, then
    rollback is automatically called. Otherwise, upon exit of the with
    block, commit is called.

    Tx blocks can be nested inside other Tx blocks. Nested Tx blocks
    never commit or rollback a transaction. Instead, the exception is
    passed along to the caller. Only the outermost transaction will
    commit or rollback the entire transaction.

    Use it like so:

        #!python
        with Tx(conn) as cursor:
            ...

    Which is meant to be roughly equivalent to the following:

        #!python
        with conn:
            with conn.cursor() as curs:
                ...

    This should only be used when you're running SQL queries directly.
    (Or when interfacing with another part of the API that requires
    a database cursor.)
    """
    def __init__(self, conn, name=None, factory=None):
        """
        `conn` is a tuple containing a `psycho_conn` and the database
        search path allowing nfldb to reside in its own schema

        `psycho_conn` is a DB connection returned from `nfldb.connect`,
        `name` is passed as the `name` argument to the cursor
        constructor (for server-side cursors), and `factory` is passed
        as the `cursor_factory` parameter to the cursor constructor.

        Note that the default cursor factory is
        `psycopg2.extras.RealDictCursor`. However, using
        `psycopg2.extensions.cursor` (the default tuple cursor) can be
        much more efficient when fetching large result sets.
        """
        psycho_conn = conn[0]
        schema = 'public'
        search_path = 'public'
        if conn[1] != 'public':
            search_path = "%s,%s" % (conn[1], search_path)
            schema = conn[1]

        tstatus = psycho_conn.get_transaction_status()
        self.__name = name
        self.__nested = tstatus == TRANSACTION_STATUS_INTRANS
        self.__conn = psycho_conn
        self.__search_path = search_path
        self.__schema = schema
        self.__cursor = None
        self.__factory = factory
        if self.__factory is None:
            self.__factory = RealDictCursor

    def __enter__(self):
        if self.__name is None:
            self.__cursor = self.__conn.cursor(cursor_factory=self.__factory)
        else:
            self.__cursor = self.__conn.cursor(name=self.__name,
                                               cursor_factory=self.__factory)
        c = self.__cursor
        c.search_path = self.__search_path
        c.schema = self.__schema
        c.execute("SET search_path TO %s" % self.__search_path)
        #class _ (object):
        #    def execute(self, *args, **kwargs):
        #        c.execute(*args, **kwargs)
        #        print(c.query)

        #    def __getattr__(self, k):
        #        return getattr(c, k)
        return c

    def __exit__(self, typ, value, traceback):
        if not self.__cursor.closed:
            self.__cursor.close()
        if typ is not None:
            if not self.__nested:
                self.__conn.rollback()
            return False
        else:
            if not self.__nested:
                self.__conn.commit()
            return True

def _big_insert(cursor, table, datas):
    """
    Given a database cursor, table name and a list of asssociation
    lists of data (column name and value), perform a single large
    insert. Namely, each association list should correspond to a single
    row in `table`.

    Each association list must have exactly the same number of columns
    in exactly the same order.
    """
    stamped = table in ('game', 'drive', 'play')
    insert_fields = [k for k, _ in datas[0]]
    if stamped:
        insert_fields.append('time_inserted')
        insert_fields.append('time_updated')
    insert_fields = ', '.join(insert_fields)

    def times(xs):
        if stamped:
            xs.append('NOW()')
            xs.append('NOW()')
        return xs

    def vals(xs):
        return [v for _, v in xs]
    values = ', '.join(_mogrify(cursor, times(vals(data))) for data in datas)

    cursor.execute(pfx(cursor, 'INSERT INTO _PFX_.%s (%s) VALUES %s')
                   % (table, insert_fields, values))


def _upsert(cursor, table, data, pk):
    """
    Performs an arbitrary "upsert" given a table, an association list
    mapping key to value, and an association list representing the
    primary key.

    Note that this is **not** free of race conditions. It is the
    caller's responsibility to avoid race conditions. (e.g., By using a
    table or row lock.)

    If the table is `game`, `drive` or `play`, then the `time_insert`
    and `time_updated` fields are automatically populated.
    """
    stamped = table in ('game', 'drive', 'play')
    update_set = ['%s = %s' % (k, '%s') for k, _ in data]
    if stamped:
        update_set.append('time_updated = NOW()')
    update_set = ', '.join(update_set)

    insert_fields = [k for k, _ in data]
    insert_places = ['%s' for _ in data]
    if stamped:
        insert_fields.append('time_inserted')
        insert_fields.append('time_updated')
        insert_places.append('NOW()')
        insert_places.append('NOW()')
    insert_fields = ', '.join(insert_fields)
    insert_places = ', '.join(insert_places)

    pk_cond = ' AND '.join(['%s = %s' % (k, '%s') for k, _ in pk])
    q = '''
        UPDATE _PFX_.%s SET %s WHERE %s;
    ''' % (table, update_set, pk_cond)
    q += '''
        INSERT INTO _PFX_.%s (%s)
        SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s)
    ''' % (table, insert_fields, insert_places, table, pk_cond)

    values = [v for _, v in data]
    pk_values = [v for _, v in pk]
    try:
        cursor.execute(pfx(cursor, q), values + pk_values + values + pk_values)
    except psycopg2.ProgrammingError as e:
        print(cursor.query)
        raise e


def _drop_stat_indexes(c):
    from nfldb.types import _play_categories, _player_categories

    for cat in _player_categories.values():
        c.execute(pfx(c, 'DROP INDEX _PFX_.play_player_in_%s') % cat)
    for cat in _play_categories.values():
        c.execute(pfx('DROP INDEX _PFX_.play_in_%s') % cat)


def _create_stat_indexes(c):
    from nfldb.types import _play_categories, _player_categories

    for cat in _player_categories.values():
        c.execute(pfx(c, 'CREATE INDEX play_player_in_%s ON _PFX_.play_player (%s ASC)')
                  % (cat, cat))
    for cat in _play_categories.values():
        c.execute(pfx(c, 'CREATE INDEX play_in_%s ON _PFX_.play (%s ASC)') % (cat, cat))

# What follows are the migration functions. They follow the naming
# convention "_migrate_{VERSION}" where VERSION is an integer that
# corresponds to the version that the schema will be after the
# migration function runs. Each migration function is only responsible
# for running the queries required to update schema. It does not
# need to update the schema version.
#
# The migration functions should accept a cursor as a parameter,
# which are created in the higher-order _migrate. In particular,
# each migration function is run in its own transaction. Commits
# and rollbacks are handled automatically.


def _migrate(conn, to):
    current = schema_version(conn)
    assert current <= to

    globs = globals()
    for v in xrange(current+1, to+1):
        fname = '_migrate_%d' % v
        with Tx(conn) as c:
            assert fname in globs, 'Migration function %d not defined.' % v
            globs[fname](c)
            c.execute(pfx(c,"UPDATE _PFX_.meta SET version = %s"), (v,))


def _migrate_1(c):
    c.execute(pfx(c, '''
        CREATE DOMAIN _PFX_.utctime AS timestamp with time zone
                          CHECK (EXTRACT(TIMEZONE FROM VALUE) = '0')
    '''))
    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.meta (
            version smallint,
            last_roster_download _PFX_.utctime NOT NULL
        )
    '''))
    c.execute(pfx(c, '''
        INSERT INTO _PFX_.meta
            (version, last_roster_download)
        VALUES (1, '0001-01-01T00:00:00Z')
    '''))


def _migrate_2(c):
    from nfldb.types import Enums, _play_categories, _player_categories
    # Create some types and common constraints.
    c.execute(pfx(c, '''
        CREATE DOMAIN _PFX_.gameid AS character varying (10)
                          CHECK (char_length(VALUE) = 10)
    '''))
    c.execute(pfx(c, '''
        CREATE DOMAIN _PFX_.usmallint AS smallint
                          CHECK (VALUE >= 0)
    '''))
    c.execute(pfx(c,'''
        CREATE DOMAIN _PFX_.game_clock AS smallint
                          CHECK (VALUE >= 0 AND VALUE <= 900)
    '''))
    c.execute(pfx(c,'''
        CREATE DOMAIN _PFX_.field_offset AS smallint
                          CHECK (VALUE >= -50 AND VALUE <= 50)
    '''))

    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.game_phase AS ENUM %s
    ''') % _mogrify(c, Enums.game_phase))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.season_phase AS ENUM %s
    ''') % _mogrify(c, Enums.season_phase))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.game_day AS ENUM %s
    ''') % _mogrify(c, Enums.game_day))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.player_pos AS ENUM %s
    ''') % _mogrify(c, Enums.player_pos))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.player_status AS ENUM %s
    ''') % _mogrify(c, Enums.player_status))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.game_time AS (
            phase _PFX_.game_phase,
            elapsed _PFX_.game_clock
        )
    '''))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.pos_period AS (
            elapsed _PFX_.usmallint
        )
    '''))
    c.execute(pfx(c, '''
        CREATE TYPE _PFX_.field_pos AS (
            pos _PFX_.field_offset
        )
    '''))

    # Now that some types have been made, add current state to meta table.
    c.execute(pfx(c, '''
        ALTER TABLE _PFX_.meta
            ADD season_type _PFX_.season_phase NULL,
            ADD season_year _PFX_.usmallint NULL
                    CHECK (season_year >= 1960 AND season_year <= 2100),
            ADD week _PFX_.usmallint NULL
                    CHECK (week >= 1 AND week <= 25)
    '''))

    # Create the team table and populate it.
    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.team (
            team_id character varying (3) NOT NULL,
            city character varying (50) NOT NULL,
            name character varying (50) NOT NULL,
            PRIMARY KEY (team_id)
        )
    '''))
    c.execute(pfx(c, '''
        INSERT INTO _PFX_.team (team_id, city, name) VALUES %s
    ''') % (', '.join(_mogrify(c, team[0:3]) for team in nfldb.team.teams)))

    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.player (
            player_id character varying (10) NOT NULL
                CHECK (char_length(player_id) = 10),
            gsis_name character varying (75) NULL,
            full_name character varying (100) NULL,
            first_name character varying (100) NULL,
            last_name character varying (100) NULL,
            team character varying (3) NOT NULL,
            position _PFX_.player_pos NOT NULL,
            profile_id integer NULL,
            profile_url character varying (255) NULL,
            uniform_number _PFX_.usmallint NULL,
            birthdate character varying (75) NULL,
            college character varying (255) NULL,
            height character varying (100) NULL,
            weight character varying (100) NULL,
            years_pro _PFX_.usmallint NULL,
            status _PFX_.player_status NOT NULL,
            PRIMARY KEY (player_id),
            FOREIGN KEY (team)
                REFERENCES _PFX_.team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    '''))
    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.game (
            gsis_id _PFX_.gameid NOT NULL,
            gamekey character varying (5) NULL,
            start_time _PFX_.utctime NOT NULL,
            week _PFX_.usmallint NOT NULL
                CHECK (week >= 1 AND week <= 25),
            day_of_week _PFX_.game_day NOT NULL,
            season_year _PFX_.usmallint NOT NULL
                CHECK (season_year >= 1960 AND season_year <= 2100),
            season_type _PFX_.season_phase NOT NULL,
            finished boolean NOT NULL,
            home_team character varying (3) NOT NULL,
            home_score _PFX_.usmallint NOT NULL,
            home_score_q1 _PFX_.usmallint NULL,
            home_score_q2 _PFX_.usmallint NULL,
            home_score_q3 _PFX_.usmallint NULL,
            home_score_q4 _PFX_.usmallint NULL,
            home_score_q5 _PFX_.usmallint NULL,
            home_turnovers _PFX_.usmallint NOT NULL,
            away_team character varying (3) NOT NULL,
            away_score _PFX_.usmallint NOT NULL,
            away_score_q1 _PFX_.usmallint NULL,
            away_score_q2 _PFX_.usmallint NULL,
            away_score_q3 _PFX_.usmallint NULL,
            away_score_q4 _PFX_.usmallint NULL,
            away_score_q5 _PFX_.usmallint NULL,
            away_turnovers _PFX_.usmallint NOT NULL,
            time_inserted _PFX_.utctime NOT NULL,
            time_updated _PFX_.utctime NOT NULL,
            PRIMARY KEY (gsis_id),
            FOREIGN KEY (home_team)
                REFERENCES _PFX_.team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE,
            FOREIGN KEY (away_team)
                REFERENCES _PFX_.team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    '''))
    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.drive (
            gsis_id _PFX_.gameid NOT NULL,
            drive_id _PFX_.usmallint NOT NULL,
            start_field _PFX_.field_pos NULL,
            start_time _PFX_.game_time NOT NULL,
            end_field _PFX_.field_pos NULL,
            end_time _PFX_.game_time NOT NULL,
            pos_team character varying (3) NOT NULL,
            pos_time _PFX_.pos_period NULL,
            first_downs _PFX_.usmallint NOT NULL,
            result text NULL,
            penalty_yards smallint NOT NULL,
            yards_gained smallint NOT NULL,
            play_count _PFX_.usmallint NOT NULL,
            time_inserted _PFX_.utctime NOT NULL,
            time_updated _PFX_.utctime NOT NULL,
            PRIMARY KEY (gsis_id, drive_id),
            FOREIGN KEY (gsis_id)
                REFERENCES _PFX_.game (gsis_id)
                ON DELETE CASCADE,
            FOREIGN KEY (pos_team)
                REFERENCES _PFX_.team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    '''))

    # I've taken the approach of using a sparse table to represent
    # sparse play statistic data. See issue #2:
    # https://github.com/BurntSushi/nfldb/issues/2
    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.play (
            gsis_id _PFX_.gameid NOT NULL,
            drive_id _PFX_.usmallint NOT NULL,
            play_id _PFX_.usmallint NOT NULL,
            time _PFX_.game_time NOT NULL,
            pos_team character varying (3) NOT NULL,
            yardline _PFX_.field_pos NULL,
            down smallint NULL
                CHECK (down >= 1 AND down <= 4),
            yards_to_go smallint NULL
                CHECK (yards_to_go >= 0 AND yards_to_go <= 100),
            description text NULL,
            note text NULL,
            time_inserted _PFX_.utctime NOT NULL,
            time_updated _PFX_.utctime NOT NULL,
            %s,
            PRIMARY KEY (gsis_id, drive_id, play_id),
            FOREIGN KEY (gsis_id, drive_id)
                REFERENCES _PFX_.drive (gsis_id, drive_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id)
                REFERENCES _PFX_.game (gsis_id)
                ON DELETE CASCADE,
            FOREIGN KEY (pos_team)
                REFERENCES _PFX_.team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''') % ', '.join([cat._sql_field for cat in _play_categories.values()]))

    c.execute(pfx(c, '''
        CREATE TABLE _PFX_.play_player (
            gsis_id _PFX_.gameid NOT NULL,
            drive_id _PFX_.usmallint NOT NULL,
            play_id _PFX_.usmallint NOT NULL,
            player_id character varying (10) NOT NULL,
            team character varying (3) NOT NULL,
            %s,
            PRIMARY KEY (gsis_id, drive_id, play_id, player_id),
            FOREIGN KEY (gsis_id, drive_id, play_id)
                REFERENCES _PFX_.play (gsis_id, drive_id, play_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id, drive_id)
                REFERENCES _PFX_.drive (gsis_id, drive_id)
                ON DELETE CASCADE,
            FOREIGN KEY (gsis_id)
                REFERENCES _PFX_.game (gsis_id)
                ON DELETE CASCADE,
            FOREIGN KEY (player_id)
                REFERENCES _PFX_.player (player_id)
                ON DELETE RESTRICT,
            FOREIGN KEY (team)
                REFERENCES _PFX_.team (team_id)
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        )
    ''') % ', '.join(cat._sql_field for cat in _player_categories.values()))


def _migrate_3(c):
    _create_stat_indexes(c)

    c.execute(pfx(c, '''
        CREATE INDEX player_in_gsis_name ON _PFX_.player (gsis_name ASC);
        CREATE INDEX player_in_full_name ON _PFX_.player (full_name ASC);
        CREATE INDEX player_in_team ON _PFX_.player (team ASC);
        CREATE INDEX player_in_position ON _PFX_.player (position ASC);
    '''))
    c.execute(pfx(c, '''
        CREATE INDEX game_in_gamekey ON _PFX_.game (gamekey ASC);
        CREATE INDEX game_in_start_time ON _PFX_.game (start_time ASC);
        CREATE INDEX game_in_week ON _PFX_.game (week ASC);
        CREATE INDEX game_in_day_of_week ON _PFX_.game (day_of_week ASC);
        CREATE INDEX game_in_season_year ON _PFX_.game (season_year ASC);
        CREATE INDEX game_in_season_type ON _PFX_.game (season_type ASC);
        CREATE INDEX game_in_finished ON _PFX_.game (finished ASC);
        CREATE INDEX game_in_home_team ON _PFX_.game (home_team ASC);
        CREATE INDEX game_in_away_team ON _PFX_.game (away_team ASC);
        CREATE INDEX game_in_home_score ON _PFX_.game (home_score ASC);
        CREATE INDEX game_in_away_score ON _PFX_.game (away_score ASC);
        CREATE INDEX game_in_home_turnovers ON _PFX_.game (home_turnovers ASC);
        CREATE INDEX game_in_away_turnovers ON _PFX_.game (away_turnovers ASC);
    '''))
    c.execute(pfx(c, '''
        CREATE INDEX drive_in_gsis_id ON _PFX_.drive (gsis_id ASC);
        CREATE INDEX drive_in_drive_id ON _PFX_.drive (drive_id ASC);
        CREATE INDEX drive_in_start_field ON _PFX_.drive
            (((start_field).pos) ASC);
        CREATE INDEX drive_in_end_field ON _PFX_.drive
            (((end_field).pos) ASC);
        CREATE INDEX drive_in_start_time ON _PFX_.drive
            (((start_time).phase) ASC, ((start_time).elapsed) ASC);
        CREATE INDEX drive_in_end_time ON _PFX_.drive
            (((end_time).phase) ASC, ((end_time).elapsed) ASC);
        CREATE INDEX drive_in_pos_team ON _PFX_.drive (pos_team ASC);
        CREATE INDEX drive_in_pos_time ON _PFX_.drive
            (((pos_time).elapsed) DESC);
        CREATE INDEX drive_in_first_downs ON _PFX_.drive (first_downs DESC);
        CREATE INDEX drive_in_penalty_yards ON _PFX_.drive (penalty_yards DESC);
        CREATE INDEX drive_in_yards_gained ON _PFX_.drive (yards_gained DESC);
        CREATE INDEX drive_in_play_count ON _PFX_.drive (play_count DESC);
    '''))
    c.execute(pfx(c, '''
        CREATE INDEX play_in_gsis_id ON _PFX_.play (gsis_id ASC);
        CREATE INDEX play_in_gsis_drive_id ON _PFX_.play (gsis_id ASC, drive_id ASC);
        CREATE INDEX play_in_time ON _PFX_.play
            (((time).phase) ASC, ((time).elapsed) ASC);
        CREATE INDEX play_in_pos_team ON _PFX_.play (pos_team ASC);
        CREATE INDEX play_in_yardline ON _PFX_.play
            (((yardline).pos) ASC);
        CREATE INDEX play_in_down ON _PFX_.play (down ASC);
        CREATE INDEX play_in_yards_to_go ON _PFX_.play (yards_to_go DESC);
    '''))
    c.execute(pfx(c, '''
        CREATE INDEX pp_in_gsis_id ON _PFX_.play_player (gsis_id ASC);
        CREATE INDEX pp_in_player_id ON _PFX_.play_player (player_id ASC);
        CREATE INDEX pp_in_gsis_drive_id ON _PFX_.play_player
            (gsis_id ASC, drive_id ASC);
        CREATE INDEX pp_in_gsis_drive_play_id ON _PFX_.play_player
            (gsis_id ASC, drive_id ASC, play_id ASC);
        CREATE INDEX pp_in_gsis_player_id ON _PFX_.play_player
            (gsis_id ASC, player_id ASC);
        CREATE INDEX pp_in_team ON _PFX_.play_player (team ASC);
    '''))


def _migrate_4(c):
    c.execute(pfx(c, '''
        UPDATE _PFX_.team SET city = 'New York' WHERE team_id IN ('NYG', 'NYJ');
        UPDATE _PFX_.team SET name = 'Giants' WHERE team_id = 'NYG';
        UPDATE _PFX_.team SET name = 'Jets' WHERE team_id = 'NYJ';
    '''))

def _migrate_5(c):
    c.execute(pfx(c, '''
        UPDATE _PFX_.player SET weight = '0', height = '0'
    '''))
    c.execute(pfx(c, '''
        ALTER TABLE _PFX_.player
            ALTER COLUMN height TYPE usmallint USING height::usmallint,
            ALTER COLUMN weight TYPE usmallint USING weight::usmallint;
    '''))
