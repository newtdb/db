import json
import re
import relstorage.adapters.postgresql
import relstorage.adapters.postgresql.mover
import relstorage.adapters.postgresql.schema

from .jsonpickle import JsonUnpickler

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
def jsonify(data):
    unpickler = JsonUnpickler(data)
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
        return None, None, None

    ghost_pickle = data[:unpickler.pos]
    state = unpickler.load()
    # xstate = xform(zoid, class_name, state)
    # if xstate is not state:
    #     state = xstate
    #     if not isinstance(state, bytes):
    #         state = json.dumps(state)

    # Remove unicode surrogate strings, as postgres utf-8
    # will reject them.
    state = unicode_surrogates.sub(' ', state)

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
        class_name, ghost_pickle, state = jsonify(data)
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
    DELETE FROM object_json WHERE zoid IN (SELECT zoid FROM temp_store);

    INSERT INTO object_json (zoid, class_name, ghost_pickle, state)
    SELECT zoid, class_name, ghost_pickle, state
    FROM temp_store_json
    """

    def move_from_temp(self, cursor, tid, txn_has_blobs):
        cursor.execute(self._move_json_sql)
        return super(Mover, self).move_from_temp(cursor, tid, txn_has_blobs)

class SchemaInstaller(
    relstorage.adapters.postgresql.schema.PostgreSQLSchemaInstaller):

    def _create_object_json(self, cursor):
        cursor.execute("""
        create table object_json (
          zoid bigint primary key,
          class_name text,
          ghost_pickle bytea,
          state jsonb);
        create index object_json_json_idx on object_json using gin (state);
        """)

    def create(self, cursor):
        super(SchemaInstaller, self).create(cursor)
        self._create_object_json(cursor)

    def update_schema(self, cursor, tables):
        if object_json not in tables:
            self._create_object_json(cursor)
