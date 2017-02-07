import logging

from . import pg_connection
from ._util import closing, table_exists, trigger_exists
from ._adapter import determine_keep_history

logger = logging.getLogger(__name__)

NOTIFY = 'newt_object_state_changed'

def non_empty_generator(gen):
    try:
        first = next(gen)
    except StopIteration:
        return None
    def it():
        yield first
        for v in gen:
            yield v
    return it()

trigger_sql = """
create function newt_notify_object_state_changed() returns trigger
as $$
begin
  perform pg_notify('%s', NEW.tid::text);
  return NEW;
end;
$$ language plpgsql;

create trigger newt_trigger_notify_object_state_changed
  after insert or update on object_state for each row
  execute procedure newt_notify_object_state_changed();
""" % NOTIFY

class Updates:

    _query = """
    select s.tid, s.zoid, state from object_state s
    where s.tid > %s UPPER order by s.tid
    """

    def __init__(self, dsn, start_tid =-1, end_tid=None,
                 batch_limit=100000, internal_batch_size=100,
                 poll_timeout=300, keep_history=None):
        self.dsn = dsn
        self.tid = start_tid
        self.end_tid = end_tid
        self.batch_limit = batch_limit
        self.internal_batch_size = internal_batch_size
        self.poll_timeout = poll_timeout
        self.keep_history = keep_history

    def _batch(self, conn):
        tid = self.tid
        try:
            updates = conn.cursor('object_state_updates')
            updates.itersize = self.internal_batch_size
            updates.execute(self._query, (tid, ))

            n = 0
            for row in updates:
                if row[0] != tid:
                    if n >= self.batch_limit:
                        break
                    tid = self.tid = row[0]
                yield row
                n += 1
        finally:
            try:
                conn.rollback()
                updates.close()
            except Exception:
                pass

    def __iter__(self):
        with closing(pg_connection(self.dsn)) as conn:
            with closing(conn.cursor()) as cursor:
                keep_history = determine_keep_history(cursor, self.keep_history)

                if keep_history:
                    self._query = self._query.replace(
                        'object_state s',
                        'object_state s natural join current_object',
                        )

                self._query = self._query.replace(
                    'UPPER',
                    '' if self.end_tid is None else
                    cursor.mogrify("and s.tid <= %s",
                                   (self.end_tid, )).decode('ascii'),
                    )

                # Catch up:
                while True:
                    batch = non_empty_generator(self._batch(conn))
                    if batch is None:
                        break # caught up
                    else:
                        yield batch

                if self.end_tid is None:
                    for payload in listen(self.dsn, True,
                                          poll_timeout=self.poll_timeout):
                        batch = non_empty_generator(self._batch(conn))
                        if batch is not None:
                            yield batch

def listen(dsn, timeout_on_start=False, poll_timeout=300):
    """Listen for newt database updates.

    Returns an iterator that returns integer transaction ids or None values.

    The purpose of this method is to determine if there are
    updates.  If transactions are committed very quickly, then not
    all of them will be returned by the iterator.

    None values indicate that ``poll_interval`` seconds have
    passed since the last update.

    Parameters:

    dsn
      A Postgres connection string

    timeout_on_start
      Force None to be returned immediately after listening for
      notifications.

      This is useful in some special cases to avoid having to time out
      waiting for changes that happened before the iterator began
      listening.

    poll_timeout
      A timeout after which None is returned if there are no changes.
      (This is a backstop to PostgreSQL's notification system.)
    """
    with closing(pg_connection(dsn)) as conn:
        conn.autocommit = True
        with closing(conn.cursor()) as cursor:
            if not trigger_exists(cursor,
                                  'newt_trigger_notify_object_state_changed'):
                cursor.execute(trigger_sql)

            cursor.execute("LISTEN " + NOTIFY)

            from select import select
            selargs = [conn], (), (), poll_timeout

            if timeout_on_start:
                # avoid a race between catching up and starting to LISTEN
                yield None

            while True:
                if select(*selargs) == ([], [], []):
                    yield None
                else:
                    conn.poll()
                    if conn.notifies:
                        if any(n.payload == 'STOP' for n in conn.notifies):
                            return # for tests
                        # yield the last
                        yield conn.notifies[-1].payload

def updates(conn, start_tid=-1, end_tid=None,
            batch_limit=100000, internal_batch_size=100,
            poll_timeout=300):
    """Create a data-update iterator

    The iterator returns an iterator of batchs, where each batch is an
    iterator of records. Each record is a triple consisting of an
    integer transaction id, integer object id and data.  A sample
    use::

      >>> import newt.db
      >>> import newt.db.follow
      >>> connection = newt.db.pg_connection('')
      >>> for batch in newt.db.follow.updates(connection):
      ...     for tid, zoid, data in batch:
      ...         print(tid, zoid, len(data))

    If no ``end_tid`` is provided, the iterator will iterate until
    interrupted.

    Parameters:

    conn
      A Postgres database connection.

    start_tid
      Start tid, expressed as an integer.  The iterator starts at the
      first transaction **after** this tid.

    end_tid
      End tid, expressed as an integer.  The iterator stops at this, or at
      the end of data, whichever is less.  If the end tid is None, the
      iterator will run indefinately, returning new data as they are
      committed.

    batch_limit
      A soft batch size limit.  When a batch reaches this limit, it will
      end at the next transaction boundary.  The purpose of this limit is to
      limit read-transaction size.

    internal_batch_size
      The size of the internal Postgres iterator.  Data aren't loaded from
      Postgres all at once.  Server-side cursors are used and data are
      loaded from the server in ``internal_batch_size`` batches.

    poll_timeout
      When no ``end_tid`` is specified, this specifies how often to poll
      for changes.  Note that a trigger is created and used to notify the
      iterator of changes, so changes ne detected quickly. The poll
      timeout is just a backstop.
    """

    return Updates(conn, start_tid, end_tid, batch_limit, internal_batch_size,
                   poll_timeout)

def _ex_progress(conn, cursor, sql, *args):
    try:
        cursor.execute(sql, args)
    except Exception:
        # Hm, maybe the table doesn't exist:
        conn.rollback()
        if not table_exists(cursor, 'newt_follow_progress'):
            cursor.execute("create table newt_follow_progress"
                           " (id text primary key, tid bigint)")

        # Try again. Note that if we didn't create the table, this
        # will hopefullt fail again, forcing the caller to rollback.
        cursor.execute(sql, args)

def get_progress_tid(connection, id):
    """Get the current progress for a follow client.

    Return the last saved integer transaction id for the client, or
    -1, if one hasn't been saved before.

    A follow client often updates some other data based on the data
    returned from ``updates``.  It may stop and restart later. To do
    this, it will call ``set_progress_tid`` to save its progress and
    later call ``get_progress_tid`` to find where it left off.  It can
    then pass the returned tid as ``start_tid`` to ``updates``.

    The ``connection`` argument must be a PostgreSQL connection string
    or connection.

    The ``id`` parameters is used to identify which progress is
    wanted.  This should uniquely identify the client and generally a
    dotted name (``__name__``) of the client module is used.  This
    allows multiple clients to have their progress tracked.
    """
    if isinstance(connection, str):
        with closing(pg_connection(connection)) as conn:
            return get_progress_tid(conn, id)

    with closing(connection.cursor()) as cursor:
        _ex_progress(
            connection, cursor,
            "select tid from newt_follow_progress where id = %s", id)

        tid = list(cursor)
        if tid:
            return tid[0][0]
        else:
            return -1

def set_progress_tid(connection, id, tid):
    """Set the current progress for a follow client.

    See ``get_progress_tid``.

    The ``connection`` argument must be a PostgreSQL connection string
    or connection.

    The ``id`` argument is a string identifying a client. It should
    generally be a dotted name (usually ``__name__``) of the client
    module.  It must uniquely identify the client.

    The ``tid`` argument is the most recently processed transaction id
    as an int.
    """
    if isinstance(connection, str):
        with closing(pg_connection(connection)) as conn:
            set_progress_tid(conn, id, tid)
            conn.commit()
            return

    with closing(connection.cursor()) as cursor:
        _ex_progress(connection, cursor,
                     "delete from newt_follow_progress where id=%s", id)
        cursor.execute(
            "insert into newt_follow_progress(id, tid) values(%s, %s)",
            (id, tid))

def stop_updates(conn):
    """Notify all ``updates`` iterators that they should stop.

    This is mainly intended for tests. It only works for iterators
    that are already waiting for for new data.
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute("notify %s, 'STOP'" % NOTIFY)

def garbage(dsn):
    with closing(pg_connection(dsn)) as conn:
        with closing(conn.cursor('find_garbage')) as cursor:
            cursor.execute("select zoid from pack_object where not keep")
            for r in cursor:
                yield r[0]
