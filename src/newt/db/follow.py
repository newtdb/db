import logging
import psycopg2

logger = logging.getLogger(__name__)

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
  perform pg_notify('newt_object_state_changed', NEW.tid::text);
  return NEW;
end;
$$ language plpgsql;

create trigger newt_trigger_notify_object_state_changed
  after insert or update on object_state for each row
  execute procedure newt_notify_object_state_changed();
"""

class Updates:

    _query = """
    select s.tid, s.zoid, state from object_state s
    where s.tid > %s and s.tid <= %s order by s.tid
    """

    def __init__(self, conn, start_tid=-1, end_tid=None,
                 batch_limit=100000, internal_batch_size=100,
                 poll_timeout=300, keep_history=None):
        self.conn = conn
        self.cursor = conn.cursor()
        self.ex = self.cursor.execute
        self.tid = start_tid
        self.follow = end_tid is None
        self.end_tid = end_tid or 1<<62
        self.poll_timeout = poll_timeout
        self.batch_limit = batch_limit
        self.internal_batch_size = internal_batch_size
        if end_tid is None:
            self.ex(
                "select 1 from pg_catalog.pg_trigger "
                "where tgname = 'newt_trigger_notify_object_state_changed'")
            if not list(self.cursor):
                self.ex(trigger_sql)
            self.conn.commit()

        if keep_history is None:
            self.ex(
                "select 1 from pg_catalog.pg_class "
                "where relname = 'current_object'")
            keep_history = bool(list(self.cursor))

        if keep_history:
            self._query = self._query.replace(
                'object_state s',
                'object_state s natural join current_object',
                )

    def _batch(self):
        tid = self.tid
        self.ex('begin')
        try:
            updates = self.conn.cursor('object_state_updates')
            updates.itersize = self.internal_batch_size
            try:
                updates.execute(self._query, (tid, self.end_tid))
            except Exception:
                logger.exception("Getting updates after %s", tid)
                self.ex('rollback')
                raise

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
                self.conn.rollback()
                updates.close()
            except Exception:
                pass

    def listen(self, timeout_on_start=False):
        """Listen for updates.

        Returns an iterator that returns integer transaction ids or None values.

        The purpose of this method is to determine if there are
        updates.  If transactions are committed very quickly, then not
        all of them will be returned by the iterator.

        None values indicate that ``poll_interval`` seconds have
        passed since the last update.
        """
        conn = psycopg2.connect(self.conn.dsn)
        try:
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            curs = conn.cursor()
            curs.execute("LISTEN newt_object_state_changed")
            from select import select
            selargs = [conn], (), (), self.poll_timeout

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
        finally:
            conn.close()

    def __iter__(self):
        # Catch up:
        while True:
            batch = non_empty_generator(self._batch())
            if batch is None:
                break # caught up
            else:
                yield batch

        if self.follow:
            for payload in self.listen(True):
                batch = non_empty_generator(self._batch())
                if batch is not None:
                    yield batch

def updates(conn, start_tid=-1, end_tid=None,
            batch_limit=100000, internal_batch_size=100,
            poll_timeout=300):
    """Create a data-update iterator

    The iterator returns an iterator of batchs, where each batch is an
    iterator of records. Each record is a triple consisting of an
    integer transaction id, integer object id and data.  A sample
    use::

      >>> import newt.db.follow
      >>> import psycopg2
      >>> connection = psycopg2.connect('')
      >>> for batch in newt.db.follow.updates(connection):
      ...     for tid, zoid, data in batch:
      ...         print(tid, zoid, len(data))

    If no ``end_tid`` is provided, the iterator will iterate until
    interrupted.

    Parameters:

    conn
      A psycopg2 database connection.

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
