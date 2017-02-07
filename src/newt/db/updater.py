from __future__ import print_function
"""Updates database json representation
"""

import argparse
import itertools
import logging
import relstorage.adapters.postgresql
import relstorage.options
import sys

from . import pg_connection
from . import follow
from .jsonpickle import Jsonifier
from ._adapter import DELETE_TRIGGER
from ._util import closing, table_exists, trigger_exists

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('connection_string', help='Postgresql connection string')
parser.add_argument('-t', '--poll-timeout', type=int, default=300,
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
    '-T', '--remove-delete-trigger', action="store_true",
    help="""\
Remove the Newt DB delete trigger, if it exists.

The Newt DB delete trigger is incompatible with the updater.  It can cause
deadlock errors is packed while the updater is running.
""")

gc_sql = """
delete from newt n where not exists (
  select from object_state s where n.zoid = s.zoid)
"""

parser.add_argument(
    '-g', '--gc-only', action="store_true",
    help="""\
Collect garbage and exit.

This removes Newt DB records that don't have corresponding database records.
This is done by executing:

%s

Note that garbage collection is normally performed on startup unless
the -G option is used.
""" % gc_sql)

parser.add_argument(
    '-G', '--no-gc', action="store_true",
    help="Don't perform garbage collection on startup.")

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

parser.add_argument(
    '--nagios',
    help="""\
Check the status of the updater.

The status is checked by checking the updater lag, which is the
difference between the last transaction committed to the database, and
the last transaction processed by the updater.  The option takes 2
numbers, separated by commas.  The first number is the lag, in
seconds, for the updater to be considered to be OK.  The second number
is the maximum lag for which the updater isn't considered to be in
error. For example, 1,99 indicates OK if 1 or less, WARNING if more
than 1 and less than or equal to 99 and ERROR of more than 99 seconds.
""")

def _update_newt(conn, cursor, jsonifier, Binary, batch):
    ex = cursor.execute
    mogrify = cursor.mogrify

    tid = None
    while True:
        data = list(itertools.islice(batch, 0, 100))
        if not data:
            break
        tid = data[-1][0]

        # Delete any existing records for the values. 2 reasons:
        # a) Make sire that new invalid data removes old valid data, and
        # b) Don't depend on upsert.
        ex("delete from newt where zoid = any(%s)", ([d[1] for d in data], ))

        # Convert, filtering out null conversions (uninteresting classes)
        to_save = []
        for tid, zoid, state in data:
            class_name, ghost_pickle, state = jsonifier((tid, zoid), state)
            if state is not None:
                to_save.append((zoid, class_name, Binary(ghost_pickle), state))

        if to_save:
            ex("insert into newt (zoid, class_name, ghost_pickle, state)"
               " values " +
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
    dsn = options.connection_string
    with closing(pg_connection(dsn)) as conn:
        with closing(conn.cursor()) as cursor:
            if options.nagios:
                if not table_exists(cursor, 'newt_follow_progress'):
                    print("Updater has not run")
                    return 2
                cursor.execute("select max(tid) from object_state")
                [[stid]] = cursor
                utid = follow.get_progress_tid(conn, __name__)
                if stid is None:
                    if utid == -1:
                        print("No transactions")
                        return 0
                    else:
                        print("Updater saw data but there was None")
                        return 2
                elif utid < 0:
                    print("Updater hasn't done anything")
                    return 2
                else:
                    from ZODB.utils import p64
                    from ZODB.TimeStamp import TimeStamp
                    lag = (TimeStamp(p64(stid)).timeTime() -
                           TimeStamp(p64(utid)).timeTime())
                    if lag < 0:
                        print("Updater is ahead")
                        return 2
                    warn, error = map(int, options.nagios.split(','))
                    flag = lambda : ("%99.3f" % lag).strip()
                    if lag > error:
                        print("Updater is too far behind | %s" % flag())
                        return 2
                    elif lag > warn:
                        print("Updater is behind | %s" % flag())
                        return 1
                    else:
                        print("OK | %s" % flag())
                        return 0

            tid = follow.get_progress_tid(conn, __name__)
            if tid < 0 and not table_exists(cursor, 'newt'):
                from ._adapter import _newt_ddl
                cursor.execute(_newt_ddl)
            elif trigger_exists(cursor, DELETE_TRIGGER):
                if options.remove_delete_trigger:
                    cursor.execute("drop trigger %s on object_state" %
                                   DELETE_TRIGGER)
                else:
                    logger.error(
                        "The Newt DB delete trigger exists.\n"
                        "It is incompatible with the updater.\n"
                        "Use -T to remove it.")
                    return 1

            if not options.no_gc:
                cursor.execute(gc_sql)

            conn.commit()

            if options.gc_only:
                if options.no_gc:
                    logger.warn(
                        "Exiting after garbage collection,\n"
                        "but garbage collection was suppressed.")
                return 0

            if options.redo:
                start_tid = -1
                end_tid = tid
                logger.info("Redoing through", tid)
            else:
                logger.info("Starting updater at %s", tid)
                start_tid = tid
                end_tid = None

            for batch in follow.updates(
                dsn,
                start_tid=start_tid,
                end_tid=end_tid,
                batch_limit=options.transaction_size_limit,
                poll_timeout=options.poll_timeout,
                ):
                _update_newt(conn, cursor, jsonifier, Binary, batch)

if __name__ == '__main__':
    sys.exit(main())
