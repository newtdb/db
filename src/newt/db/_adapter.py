import json
import logging
import re
import relstorage.adapters.postgresql
import relstorage.adapters.postgresql.mover
import relstorage.adapters.postgresql.schema

from .jsonpickle import JsonUnpickler

logger = logging.getLogger(__name__)

class Adapter(relstorage.adapters.postgresql.PostgreSQLAdapter):

    def __init__(self, *args, **kw):
        super(Adapter, self).__init__(*args, **kw)

        driver = relstorage.adapters.postgresql.select_driver(self.options)
        self.schema = SchemaInstaller(
            connmanager=self.connmanager,
            runner=self.runner,
            locker=self.locker,
            keep_history=self.keep_history,
        )
        self.mover = Mover(
            database_type='postgresql',
            options=self.options,
            runner=self.runner,
            version_detector=self.version_detector,
            Binary=driver.Binary,
        )
        self.connmanager.set_on_store_opened(self.mover.on_store_opened)

skip_class = re.compile('BTrees[.]|ZODB.blob').match
unicode_surrogates = re.compile(r'\\ud[89a-f][0-9a-f]{2,2}', flags=re.I)
NoneNoneNone = None, None, None
def jsonify(oid, data):
    if not data:
        return NoneNoneNone
    unpickler = JsonUnpickler(data)
    try:
        klass = json.loads(unpickler.load())

        if isinstance(klass, list):
            klass, args = klass
            if isinstance(klass, list):
                class_name = '.'.join(klass)
            else:
                class_name = klass['name']
        else:
            class_name = klass['name']

        if skip_class(class_name):
            return NoneNoneNone

        ghost_pickle = data[:unpickler.pos]
        state = unpickler.load()

        # xstate = xform(zoid, class_name, state)
        # if xstate is not state:
        #     state = xstate
        #     if not isinstance(state, bytes):
        #         state = json.dumps(state)

        # Remove unicode surrogate strings, as postgres utf-8
        # will reject them.

        state = unicode_surrogates.sub(' ', state).replace('\\u0000', ' ')
    except Exception:
        logger.warn("Failed pickle load, oid: %r, pickle starts: %r",
                    oid, data[:50], exc_info=True)
        return NoneNoneNone

    return class_name, ghost_pickle, state


class Mover(relstorage.adapters.postgresql.mover.PostgreSQLObjectMover):

    def on_store_opened(self, cursor, restart=False):
        super(Mover, self).on_store_opened(cursor, restart)
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_store_json (
                zoid         BIGINT NOT NULL,
                class_name   TEXT,
                ghost_pickle BYTEA,
                state        JSONB
            ) ON COMMIT DROP""")

    def store_temp(self, cursor, batcher, oid, prev_tid, data):
        super(Mover, self).store_temp(cursor, batcher, oid, prev_tid, data)
        class_name, ghost_pickle, state = jsonify(oid, data)
        if class_name is None:
            return
        batcher.delete_from('temp_store_json', zoid=oid)
        batcher.insert_into(
            "temp_store_json (zoid, class_name, ghost_pickle, state)",
            "%s, %s, %s, %s",
            (oid, class_name, self.Binary(ghost_pickle), state),
            rowkey=oid,
            size=len(state),
            )

    _move_json_sql = """
    DELETE FROM newt WHERE zoid IN (SELECT zoid FROM temp_store);

    INSERT INTO newt (zoid, class_name, ghost_pickle, state)
    SELECT zoid, class_name, ghost_pickle, state
    FROM temp_store_json
    """

    def move_from_temp(self, cursor, tid, txn_has_blobs):
        r = super(Mover, self).move_from_temp(cursor, tid, txn_has_blobs)
        cursor.execute(self._move_json_sql)
        return r

    def restore(self, cursor, batcher, oid, tid, data):
        super(Mover, self).restore(cursor, batcher, oid, tid, data)
        class_name, ghost_pickle, state = jsonify(oid, data)
        if class_name is None:
            return
        batcher.delete_from('newt', zoid=oid)
        batcher.insert_into(
            "newt (zoid, class_name, ghost_pickle, state)",
            "%s, %s, %s, %s",
            (oid, class_name, self.Binary(ghost_pickle), state),
            rowkey=oid,
            size=len(state),
            )

_newt_delete_on_state_delete = """
create function newt_delete_on_state_delete() returns trigger
as $$
begin
  delete from newt where zoid = OLD.zoid;
  return old;
end;
$$ language plpgsql;
"""

_newt_delete_on_state_delete_HP = """
create function newt_delete_on_state_delete() returns trigger
as $$
declare
  current_tid bigint;
begin
  select tid from current_object where zoid = OLD.zoid into current_tid;
  if current_tid is null or current_tid = OLD.tid then
    delete from newt where zoid = OLD.zoid;
  end if;
  return OLD;
end;
$$ language plpgsql;
"""

def _create_newt_delete_trigger(cursor, keep_history):
    cursor.execute(
        _newt_delete_on_state_delete_HP if keep_history else
        _newt_delete_on_state_delete)
    cursor.execute("""
    create trigger newt_delete_on_state_delete_trigger
      after delete on object_state for each row
      execute procedure newt_delete_on_state_delete();
    """)

def create_newt(cursor, keep_history):
    cursor.execute("""
    create table newt (
      zoid bigint primary key,
      class_name text,
      ghost_pickle bytea,
      state jsonb);
    create index newt_json_idx on newt using gin (state);
    """)
    _create_newt_delete_trigger(cursor, keep_history)

class SchemaInstaller(
    relstorage.adapters.postgresql.schema.PostgreSQLSchemaInstaller):

    def _create_newt(self, cursor):
        cursor.execute("""
        create table newt (
          zoid bigint primary key,
          class_name text,
          ghost_pickle bytea,
          state jsonb);
        create index newt_json_idx on newt using gin (state);
        """)

    def create(self, cursor):
        super(SchemaInstaller, self).create(cursor)
        create_newt(cursor, self.keep_history)

    def update_schema(self, cursor, tables):
        if 'newt' not in tables:
            self._create_newt(cursor)
        cursor.execute(
            "select 1 from pg_catalog.pg_trigger "
            "where tgname = 'newt_delete_on_state_delete_trigger'")
        if not list(cursor):
            _create_newt_delete_trigger(cursor, self.keep_history)

    def drop_all(self):
        def callback(_conn, cursor):
            cursor.execute("drop table if exists newt")
            cursor.execute(
                "drop function if exists newt_delete_on_state_delete() cascade"
                )
        self.connmanager.open_and_call(callback)
        super(SchemaInstaller, self).drop_all()
