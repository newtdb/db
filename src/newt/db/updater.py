from __future__ import print_function
"""Updates database json representation
"""

import argparse
import itertools
import logging
import relstorage.adapters.postgresql
import relstorage.options

from . import pg_connection
from . import follow
from .jsonpickle import Jsonifier
from ._util import closing, table_exists

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('connection_string', help='Postgresql connection string')
parser.add_argument('-t', '--poll-timeout', type=int, default=30,
                    help='Change-poll timeout, in seconds')
parser.add_argument('-m', '--transaction-size-limit', type=int, default=100000,
                    help='Transaction size limit (aproximate)')
parser.add_argument(
    '-l', '--logging-configuration', default='info',
    help='Logging configuration file path, or a logging level name')

parser.add_argument(
    '-d', '--driver', default='auto',
    help='Provide an explicit Postgres driver name (e.g. psycopg2)')

parser.add_argument(
    '--redo', action='store_true',
    help="""\
Redo updates

Rather than processing records written before the current tid (in
object_json_tid), process records writen up through the current tid
and stop.

This is used to update records after changes to data
transformations. It should be run *after* restarting the regulsr
updater.
""")

insert_sql = """
insert into newt (zoid, class_name, ghost_pickle, state)
values %s
on conflict (zoid)
do update set class_name   = excluded.class_name,
              ghost_pickle = excluded.ghost_pickle,
              state        = excluded.state
"""

def update_newt(conn, cursor, jsonifier, Binary, batch):
    ex = cursor.execute
    mogrify = cursor.mogrify

    tid = None
    while True:
        data = list(itertools.islice(batch, 0, 100))
        if not data:
            break
        tid = data[-1][0]

        # Convert, filtering out null conversions (uninteresting classes)
        to_save = []
        for tid, zoid, state in data:
            class_name, ghost_pickle, state = jsonifier((tid, zoid), state)
            if state is not None:
                to_save.append((zoid, class_name, Binary(ghost_pickle), state))

        if to_save:
            ex(insert_sql %
               ', '.join(mogrify('(%s, %s, %s, %s)', d).decode('ascii')
                         for d in to_save)
               )

    if tid is not None:
        follow.set_progress_tid(conn, __name__, tid)

    conn.commit()


logging_levels = 'DEBUG INFO WARNING ERROR CRITICAL'.split()

def main(args=None):
    options = parser.parse_args(args)

    if options.logging_configuration.upper() in logging_levels:
        logging.basicConfig(level=options.logging_configuration.upper())
    else:
        with open(options.logging_configuration) as f:
            from ZConfig import configureLoggers
            configureLoggers(f.read())

    jsonifier = Jsonifier()
    driver = relstorage.adapters.postgresql.select_driver(
        relstorage.options.Options(driver=options.driver))
    Binary = driver.Binary
    conn, cursor = driver.connect_with_isolation(
        driver.ISOLATION_LEVEL_SERIALIZABLE,
        options.connection_string)
    with closing(conn):
        with closing(cursor):
            tid = follow.get_progress_tid(conn, __name__)
            if tid < 0 and not table_exists(conn, 'newt'):
                from ._adapter import newt_ddl
                cursor.execute(newt_ddl)
            conn.commit()
            if options.redo:
                start_tid = -1
                end_tid = tid
                logger.info("Redoing through", tid)
            else:
                logger.info("Starting updater at %s", tid)
                start_tid = tid
                end_tid = None

            for batch in follow.updates(
                conn,
                start_tid=start_tid,
                end_tid=end_tid,
                batch_limit=options.transaction_size_limit,
                poll_timeout=options.poll_timeout,
                ):
                update_newt(conn, cursor, jsonifier, Binary, batch)

if __name__ == '__main__':
    main()
