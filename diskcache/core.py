"""Core disk and file backed cache API.

"""
# pylint: disable=not-async-context-manager,missing-docstring,too-many-public-methods,too-many-instance-attributes
import asyncio
import codecs
import errno
import os
import os.path as op
import pickle
import pickletools
import sqlite3
import struct
import threading
import time
import uuid
import warnings
import zlib
from io import BytesIO  # pylint: disable=ungrouped-imports
from threading import get_ident

import aiofiles
import aiosqlite

from .memo import memoize

try:
    import uvloop

    if not isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop")
except:
    pass

try:
    from contextlib import asynccontextmanager
except:
    from aiotools import actxmgr as asynccontextmanager


try:
    WindowsError
except NameError:

    class WindowsError(Exception):
        "Windows error place-holder on platforms without support."


class Constant(tuple):
    "Pretty display of immutable constant."

    def __new__(cls, name):
        return tuple.__new__(cls, (name,))

    def __repr__(self):
        return "%s" % self[0]


DBNAME = "cache.db"
ENOVAL = Constant("ENOVAL")
UNKNOWN = Constant("UNKNOWN")

MODE_NONE = 0
MODE_RAW = 1
MODE_BINARY = 2
MODE_TEXT = 3
MODE_PICKLE = 4

DEFAULT_SETTINGS = {
    u"statistics": 0,  # False
    u"tag_index": 0,  # False
    u"eviction_policy": u"least-frequently-used",
    u"size_limit": 2 ** 30,  # 1gb
    u"cull_limit": 10,
    u"sqlite_auto_vacuum": 1,  # FULL
    u"sqlite_cache_size": 2 ** 13,  # 8,192 pages
    u"sqlite_journal_mode": u"wal",
    u"sqlite_mmap_size": 2 ** 26,  # 64mb
    u"sqlite_synchronous": 1,  # NORMAL
    u"disk_min_file_size": 2 ** 15,  # 32kb
    u"disk_pickle_protocol": pickle.HIGHEST_PROTOCOL,
}

METADATA = {u"count": 0, u"size": 0, u"hits": 0, u"misses": 0}

EVICTION_POLICY = {
    "none": {"init": None, "get": None, "cull": None},
    "least-recently-stored": {
        "init": "CREATE INDEX IF NOT EXISTS Cache_store_time ON Cache (store_time)",
        "get": None,
        "cull": "SELECT {fields} FROM Cache ORDER BY store_time LIMIT ?",
    },
    "least-recently-used": {
        "init": "CREATE INDEX IF NOT EXISTS Cache_access_time ON Cache (access_time)",
        "get": "access_time = {now}",
        "cull": "SELECT {fields} FROM Cache ORDER BY access_time LIMIT ?",
    },
    "least-frequently-used": {
        "init": "CREATE INDEX IF NOT EXISTS Cache_access_count ON Cache (access_count)",
        "get": "access_count = access_count + 1",
        "cull": "SELECT {fields} FROM Cache ORDER BY access_count LIMIT ?",
    },
}


class Disk(object):
    "Cache key and value serialization for SQLite database and files."

    def __init__(self, directory, min_file_size=0, pickle_protocol=0):
        """Initialize disk instance.

        :param str directory: directory path
        :param int min_file_size: minimum size for file use
        :param int pickle_protocol: pickle protocol for serialization

        """
        self._directory = directory
        self.min_file_size = min_file_size
        self.pickle_protocol = pickle_protocol

    def hash(self, key):
        """Compute portable hash for `key`.

        :param key: key to hash
        :return: hash value

        """
        mask = 0xFFFFFFFF
        disk_key, _ = self.put(key)
        type_disk_key = type(disk_key)

        if type_disk_key is sqlite3.Binary:
            return zlib.adler32(disk_key) & mask
        elif type_disk_key is str:
            return (
                zlib.adler32(disk_key.encode("utf-8")) & mask
            )  # pylint: disable=no-member
        elif type_disk_key is int:
            return disk_key % mask
        else:
            assert type_disk_key is float
            return zlib.adler32(struct.pack("!d", disk_key)) & mask

    def put(self, key):
        """Convert `key` to fields key and raw for Cache table.

        :param key: key to convert
        :return: (database key, raw boolean) pair

        """
        # pylint: disable=bad-continuation,unidiomatic-typecheck
        type_key = type(key)

        if type_key is bytes:
            return sqlite3.Binary(key), True
        elif (
            (type_key is str)
            or (type_key is int and -9223372036854775808 <= key <= 9223372036854775807)
            or (type_key is float)
        ):
            return key, True
        else:
            data = pickle.dumps(key, protocol=self.pickle_protocol)
            result = pickletools.optimize(data)
            return sqlite3.Binary(result), False

    def get(self, key, raw):
        """Convert fields `key` and `raw` from Cache table to key.

        :param key: database key to convert
        :param bool raw: flag indicating raw database storage
        :return: corresponding Python key

        """
        # pylint: disable=no-self-use,unidiomatic-typecheck
        if raw:
            return bytes(key) if type(key) is sqlite3.Binary else key
        else:
            return pickle.load(BytesIO(key))

    def size(self, key):
        _, full_path = self.filename(key)

        return op.getsize(full_path)

    async def store(self, value, read, key=UNKNOWN):
        """Convert `value` to fields size, mode, filename, and value for Cache
        table.

        :param value: value to convert
        :param bool read: True when value is file-like object
        :param key: key for item (default UNKNOWN)
        :return: (size, mode, filename, value) tuple for Cache table

        """
        # pylint: disable=unidiomatic-typecheck
        type_value = type(value)
        min_file_size = self.min_file_size
        if type_value is bytes:
            if len(value) < min_file_size:
                return 0, MODE_RAW, None, sqlite3.Binary(value)
            else:
                filename, full_path = self.filename(key, value)

                async with aiofiles.open(full_path, "wb") as writer:
                    await writer.write(value)

                return len(value), MODE_BINARY, filename, None
        elif (
            (type_value is str and len(value) < min_file_size)
            or (
                type_value is int
                and -9223372036854775808 <= value <= 9223372036854775807
            )
            or (type_value is float)
        ):
            return 0, MODE_RAW, None, value
        elif type_value is str:
            filename, full_path = self.filename(key, value)

            async with aiofiles.open(full_path, "w", encoding="UTF-8") as writer:
                await writer.write(value)

            size = op.getsize(full_path)
            return size, MODE_TEXT, filename, None
        elif read:
            size = 0
            filename, full_path = self.filename(key, value)

            async with open(full_path, "wb") as writer:
                chunk = await value.read(2 * 22)
                while chunk != b"":
                    size += len(chunk)
                    await writer.write(chunk)
                    chunk = await value.read(2 * 22)

            return size, MODE_BINARY, filename, None
        else:
            result = pickle.dumps(value, protocol=self.pickle_protocol)

            if len(result) < min_file_size:
                return 0, MODE_PICKLE, None, sqlite3.Binary(result)
            else:
                filename, full_path = self.filename(key, value)

                async with aiofiles.open(full_path, "wb") as writer:
                    await writer.write(result)

                return len(result), MODE_PICKLE, filename, None

    async def fetch(self, mode, filename, value, read):
        """Convert fields `mode`, `filename`, and `value` from Cache table to
        value.

        :param int mode: value mode raw, binary, text, or pickle
        :param str filename: filename of corresponding value
        :param value: database value
        :param bool read: when True, return an open file handle
        :return: corresponding Python value

        """
        # pylint: disable=no-self-use,unidiomatic-typecheck
        if mode == MODE_RAW:
            return bytes(value) if type(value) is sqlite3.Binary else value
        elif mode == MODE_BINARY:
            if read:
                return aiofiles.open(op.join(self._directory, filename), "rb")
            else:
                async with aiofiles.open(
                    op.join(self._directory, filename), "rb"
                ) as reader:
                    return await reader.read()
        elif mode == MODE_TEXT:
            full_path = op.join(self._directory, filename)
            async with aiofiles.open(full_path, "r", encoding="UTF-8") as reader:
                return await reader.read()
        elif mode == MODE_PICKLE:
            if value is None:
                async with aiofiles.open(
                    op.join(self._directory, filename), "rb"
                ) as reader:
                    return pickle.loads(await reader.read())
            else:
                return pickle.load(BytesIO(value))

    def filename(self, key=UNKNOWN, value=UNKNOWN):
        """Return filename and full-path tuple for file storage.

        Filename will be a randomly generated 28 character hexadecimal string
        with ".val" suffixed. Two levels of sub-directories will be used to
        reduce the size of directories. On older filesystems, lookups in
        directories with many files may be slow.

        The default implementation ignores the `key` and `value` parameters.

        In some scenarios, for example :meth:`Cache.push
        <diskcache.Cache.push>`, the `key` or `value` may not be known when the
        item is stored in the cache.

        :param key: key for item (default UNKNOWN)
        :param value: value for item (default UNKNOWN)

        """
        # pylint: disable=unused-argument
        hex_name = uuid.uuid4().hex
        sub_dir = op.join(hex_name[:2], hex_name[2:4])
        name = hex_name[4:] + ".val"
        directory = op.join(self._directory, sub_dir)

        try:
            os.makedirs(directory)
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise

        filename = op.join(sub_dir, name)
        full_path = op.join(self._directory, filename)
        return filename, full_path

    def remove(self, filename):
        """Remove a file given by `filename`.

        This method is cross-thread and cross-process safe. If an "error no
        entry" occurs, it is suppressed.

        :param str filename: relative path to file

        """
        full_path = op.join(self._directory, filename)

        try:
            os.remove(full_path)
        except WindowsError:
            pass
        except OSError as error:
            if error.errno != errno.ENOENT:
                # ENOENT may occur if two caches attempt to delete the same
                # file at the same time.
                raise


class Timeout(Exception):
    "Database timeout expired."


class UnknownFileWarning(UserWarning):
    "Warning used by Cache.check for unknown files."


class EmptyDirWarning(UserWarning):
    "Warning used by Cache.check for empty directories."


class Cache(object):
    "Disk and file backed cache."
    # pylint: disable=bad-continuation
    def __init__(self, directory, timeout=60, disk=Disk, **settings):
        """Initialize cache instance.

        :param str directory: cache directory
        :param float timeout: SQLite connection timeout
        :param disk: Disk type or subclass for serialization
        :param settings: any of DEFAULT_SETTINGS

        """
        try:
            assert issubclass(disk, Disk)
        except (TypeError, AssertionError):
            raise ValueError("disk must subclass diskcache.Disk")

        self._directory = directory
        self.__timeout = timeout  # Manually handle retries during initialization.
        self._timeout = 0  # Manually handle retries during initialization.
        self._local = threading.local()
        self._txn_id = None
        self._disk = None
        self._page_size = None
        self.settings = settings
        self.diskclass = disk
        self.initialized = False
        self.lock = None

        if not op.isdir(directory):
            try:
                os.makedirs(directory, 0o755)
            except OSError as error:
                if error.errno != errno.EEXIST:
                    raise EnvironmentError(
                        error.errno,
                        'Cache directory "%s" does not exist'
                        " and could not be created" % self._directory,
                    )

    async def init(self):
        # Setup Settings table.
        if self.initialized:
            return

        self.initialized = True
        self.lock = asyncio.Lock()

        try:
            current_settings = dict(
                await self.fetchall("SELECT key, value FROM Settings", retry=True)
            )
        except sqlite3.OperationalError:
            current_settings = {}

        sets = DEFAULT_SETTINGS.copy()
        sets.update(current_settings)
        sets.update(self.settings)

        for key in METADATA:
            sets.pop(key, None)

        # Chance to set pragmas before any tables are created.

        await self.execute(
            "CREATE TABLE IF NOT EXISTS Settings ("
            " key TEXT NOT NULL UNIQUE,"
            " value)",
            retry=True,
        )

        for key, value in sorted(sets.items()):
            if key.startswith("sqlite_"):
                await self.reset(key, value, update=False)

        # Setup Disk object (must happen after settings initialized).

        kwargs = {
            key[5:]: value for key, value in sets.items() if key.startswith("disk_")
        }
        self._disk = self.diskclass(self._directory, **kwargs)

        # Set cached attributes: updates settings and sets pragmas.

        for key, value in sets.items():
            query = "INSERT OR REPLACE INTO Settings VALUES (?, ?)"
            await self.execute(query, (key, value), retry=True)
            await self.reset(key, value)

        for key, value in METADATA.items():
            query = "INSERT OR IGNORE INTO Settings VALUES (?, ?)"
            await self.execute(query, (key, value), retry=True)
            await self.reset(key)

        (self._page_size,), = await self.fetchall("PRAGMA page_size", retry=True)

        # Setup Cache table.

        await self.execute(
            "CREATE TABLE IF NOT EXISTS Cache ("
            " rowid INTEGER PRIMARY KEY,"
            " key BLOB,"
            " raw INTEGER,"
            " store_time REAL,"
            " expire_time REAL,"
            " access_time REAL,"
            " access_count INTEGER DEFAULT 0,"
            " tag BLOB,"
            " size INTEGER DEFAULT 0,"
            " mode INTEGER DEFAULT 0,"
            " filename TEXT,"
            " value BLOB)",
            retry=True,
        )

        await self.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS Cache_key_raw ON Cache(key, raw)",
            retry=True,
        )

        await self.execute(
            "CREATE INDEX IF NOT EXISTS Cache_expire_time ON Cache (expire_time)",
            retry=True,
        )

        query = EVICTION_POLICY[self.eviction_policy]["init"]

        if query is not None:
            await self.execute(query, retry=True)

        # Use triggers to keep Metadata updated.

        await self.execute(
            "CREATE TRIGGER IF NOT EXISTS Settings_count_insert"
            " AFTER INSERT ON Cache FOR EACH ROW BEGIN"
            " UPDATE Settings SET value = value + 1"
            ' WHERE key = "count"; END',
            retry=True,
        )

        await self.execute(
            "CREATE TRIGGER IF NOT EXISTS Settings_count_delete"
            " AFTER DELETE ON Cache FOR EACH ROW BEGIN"
            " UPDATE Settings SET value = value - 1"
            ' WHERE key = "count"; END',
            retry=True,
        )

        await self.execute(
            "CREATE TRIGGER IF NOT EXISTS Settings_size_insert"
            " AFTER INSERT ON Cache FOR EACH ROW BEGIN"
            " UPDATE Settings SET value = value + NEW.size"
            ' WHERE key = "size"; END',
            retry=True,
        )

        await self.execute(
            "CREATE TRIGGER IF NOT EXISTS Settings_size_update"
            " AFTER UPDATE ON Cache FOR EACH ROW BEGIN"
            " UPDATE Settings"
            " SET value = value + NEW.size - OLD.size"
            ' WHERE key = "size"; END',
            retry=True,
        )

        await self.execute(
            "CREATE TRIGGER IF NOT EXISTS Settings_size_delete"
            " AFTER DELETE ON Cache FOR EACH ROW BEGIN"
            " UPDATE Settings SET value = value - OLD.size"
            ' WHERE key = "size"; END',
            retry=True,
        )

        # Create tag index if requested.

        if self.tag_index:  # pylint: disable=no-member
            await self.create_tag_index()
        else:
            await self.drop_tag_index()

        # Close and re-open database connection with given timeout.

        await self.close()
        self._timeout = self.__timeout
        await self._sql_executor()  # pylint: disable=pointless-statement

    @property
    def directory(self):
        """Cache directory."""
        return self._directory

    @property
    def timeout(self):
        """SQLite connection timeout value in seconds."""
        return self._timeout

    @property
    def disk(self):
        """Disk used for serialization."""
        return self._disk

    async def db_connection(self):
        # Check process ID to support process forking. If the process
        # ID changes, close the connection and update the process ID.

        local_pid = getattr(self._local, "pid", None)
        pid = os.getpid()

        if local_pid != pid:
            await self.close()
            self._local.pid = pid

        con = getattr(self._local, "con", None)

        if con is None:
            con = self._local.con = await aiosqlite.connect(
                op.join(self._directory, DBNAME),
                timeout=self._timeout,
                isolation_level=None,
            )

            # Some SQLite pragmas work on a per-connection basis so
            # query the Settings table and reset the pragmas. The
            # Settings table may not exist so catch and ignore the
            # OperationalError that may occur.

            try:
                async with con.execute("SELECT key, value FROM Settings") as settings:
                    async for key, value in settings:
                        if key.startswith("sqlite_"):
                            await self.reset(key, value, update=False)
            except sqlite3.OperationalError:
                pass

        return con

    async def fetchall(self, query, args=None, retry=False, sql=None):
        if not retry:
            sql = sql or await self._sql_executor()
        else:
            sql = sql or self._sql_retry

        cursor = await sql(query, args)
        res = []
        if cursor:
            res = await cursor.fetchall()
            await cursor.close()

        return res

    async def fetchone(self, query, args=None, retry=False, sql=None):
        if not retry:
            sql = sql or await self._sql_executor()
        else:
            sql = sql or self._sql_retry

        cursor = await sql(query, args)
        res = None
        if cursor:
            res = await cursor.fetchall()
            await cursor.close()

        return res

    async def execute(self, query, args=None, retry=False, sql=None):
        if not retry:
            sql = sql or await self._sql_executor()
        else:
            sql = sql or self._sql_retry

        cursor = await sql(query, args)
        if cursor:
            await cursor.close()

    async def _sql_executor(self):
        return (await self.db_connection()).execute

    async def _sql_retry(self, statement, *args, **kwargs):

        # 2018-11-01 GrantJ - Some SQLite builds/versions handle
        # the SQLITE_BUSY return value and connection parameter
        # "timeout" differently. For a more reliable duration,
        # manually retry the statement for 60 seconds. Only used
        # by statements which modify the database and do not use
        # a transaction (like those in ``__init__`` or ``reset``).
        # See Issue #85 for and tests/issue_85.py for more details.

        start = time.time()
        sql = await self._sql_executor()
        while True:
            try:
                return await sql(statement, *args, **kwargs)
            except sqlite3.OperationalError as exc:
                if str(exc) != "database is locked":
                    raise
                diff = time.time() - start
                if diff > 60:
                    raise
                await asyncio.sleep(0.001)

    @asynccontextmanager
    async def transact(self, retry=False):
        """Context manager to perform a transaction by locking the cache.

        While the cache is locked, no other write operation is permitted.
        Transactions should therefore be as short as possible. Read and write
        operations performed in a transaction are atomic. Read operations may
        occur concurrent to a transaction.

        Transactions may be nested and may not be shared between threads.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        >>> cache = Cache('/tmp/diskcache')
        >>> _ = cache.clear()
        >>> with cache.transact():  # Atomically increment two keys.
        ...     _ = cache.incr('total', 123.4)
        ...     _ = cache.incr('count', 1)
        >>> with cache.transact():  # Atomically calculate average.
        ...     average = cache['total'] / cache['count']
        >>> average
        123.4

        :param bool retry: retry if database timeout occurs (default False)
        :return: context manager for use in `with` statement
        :raises Timeout: if database timeout occurs

        """
        async with self._transact(retry=retry):
            yield

    @asynccontextmanager
    async def _transact(self, retry=False, filename=None):
        sql = await self._sql_executor()
        filenames = []
        _disk_remove = self._disk.remove
        tid = get_ident()
        txn_id = self._txn_id

        if tid == txn_id:
            begin = False
        else:
            while True:
                try:
                    await self.execute("BEGIN IMMEDIATE", sql=sql)
                    begin = True
                    self._txn_id = tid
                    break
                except sqlite3.OperationalError:
                    if retry:
                        continue
                    if filename is not None:
                        _disk_remove(filename)
                    raise Timeout

        try:
            yield sql, filenames.append
        except BaseException:
            if begin:
                assert self._txn_id == tid
                self._txn_id = None
                await self.execute("ROLLBACK", sql=sql)
            raise
        else:
            if begin:
                assert self._txn_id == tid
                self._txn_id = None
                await self.execute("COMMIT", sql=sql)
            for name in filenames:
                if name is not None:
                    _disk_remove(name)

    async def set(self, key, value, expire=None, read=False, tag=None, retry=False):
        """Set `key` and `value` item in cache.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param value: value for item
        :param float expire: seconds until item expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param str tag: text to associate with key (default None)
        :param bool retry: retry if database timeout occurs (default False)
        :return: True if item was set
        :raises Timeout: if database timeout occurs

        """
        now = time.time()
        db_key, raw = self._disk.put(key)
        expire_time = None if expire is None else now + expire
        size, mode, filename, db_value = await self._disk.store(value, read, key=key)
        columns = (expire_time, tag, size, mode, filename, db_value)

        # The order of SELECT, UPDATE, and INSERT is important below.
        #
        # Typical cache usage pattern is:
        #
        # value = cache.get(key)
        # if value is None:
        #     value = expensive_calculation()
        #     cache.set(key, value)
        #
        # Cache.get does not evict expired keys to avoid writes during lookups.
        # Commonly used/expired keys will therefore remain in the cache making
        # an UPDATE the preferred path.
        #
        # The alternative is to assume the key is not present by first trying
        # to INSERT and then handling the IntegrityError that occurs from
        # violating the UNIQUE constraint. This optimistic approach was
        # rejected based on the common cache usage pattern.
        #
        # INSERT OR REPLACE aka UPSERT is not used because the old filename may
        # need cleanup.

        async with self._transact(retry, filename) as (sql, cleanup):
            rows = await self.fetchall(
                "SELECT rowid, filename FROM Cache WHERE key = ? AND raw = ?",
                (db_key, raw),
                sql=sql,
            )

            if rows:
                (rowid, old_filename), = rows
                cleanup(old_filename)
                await self._row_update(rowid, now, columns)
            else:
                await self._row_insert(db_key, raw, now, columns)

            await self._cull(now, sql, cleanup)

            return True

    async def _row_update(self, rowid, now, columns, sql=None):
        expire_time, tag, size, mode, filename, value = columns
        await self.execute(
            "UPDATE Cache SET"
            " store_time = ?,"
            " expire_time = ?,"
            " access_time = ?,"
            " access_count = ?,"
            " tag = ?,"
            " size = ?,"
            " mode = ?,"
            " filename = ?,"
            " value = ?"
            " WHERE rowid = ?",
            (
                now,  # store_time
                expire_time,
                now,  # access_time
                0,  # access_count
                tag,
                size,
                mode,
                filename,
                value,
                rowid,
            ),
            sql=sql,
        )

    async def _row_insert(self, key, raw, now, columns, sql=None):
        expire_time, tag, size, mode, filename, value = columns
        await self.execute(
            "INSERT INTO Cache("
            " key, raw, store_time, expire_time, access_time,"
            " access_count, tag, size, mode, filename, value"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                key,
                raw,
                now,  # store_time
                expire_time,
                now,  # access_time
                0,  # access_count
                tag,
                size,
                mode,
                filename,
                value,
            ),
            sql=sql,
        )

    async def _row_upsert(self, key, raw, now, columns, sql=None):
        expire_time, tag, size, mode, filename, value = columns
        await self.execute(
            """INSERT INTO Cache(
                 key, raw, store_time, expire_time, access_time,
                 access_count, tag, size, mode, filename, value
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key, raw)
                DO UPDATE SET (
                    store_time=excluded.store_time,
                    expire_time=excluded.expire_time,
                    access_time=excluded.access_time,
                    access_count=excluded.access_count,
                    tag=excluded.tag,
                    size=excluded.size,
                    mode=excluded.mode,
                    filename=excluded.filename,
                    value=excluded.value
                )
            """,
            (
                key,
                raw,
                now,  # store_time
                expire_time,
                now,  # access_time
                0,  # access_count
                tag,
                size,
                mode,
                filename,
                value,
            ),
            sql=sql,
        )

    async def _cull(self, now, sql, cleanup, limit=None):
        cull_limit = self.cull_limit if limit is None else limit

        if cull_limit == 0:
            return

        # Evict expired keys.

        select_expired_template = (
            "SELECT %s FROM Cache"
            " WHERE expire_time IS NOT NULL AND expire_time < ?"
            " ORDER BY expire_time LIMIT ?"
        )

        select_expired = select_expired_template % "filename"
        rows = await self.fetchall(select_expired, (now, cull_limit), sql=sql)

        if rows:
            delete_expired = "DELETE FROM Cache WHERE rowid IN (%s)" % (
                select_expired_template % "rowid"
            )
            await self.execute(delete_expired, (now, cull_limit), sql=sql)

            for (filename,) in rows:
                cleanup(filename)

            cull_limit -= len(rows)

            if cull_limit == 0:
                return

        # Evict keys by policy.

        select_policy = EVICTION_POLICY[self.eviction_policy]["cull"]

        if select_policy is None or (await self.volume()) < self.size_limit:
            return

        select_filename = select_policy.format(fields="filename", now=now)
        rows = await self.fetchall(select_filename, (cull_limit,), sql=sql)

        if rows:
            delete = "DELETE FROM Cache WHERE rowid IN (%s)" % (
                select_policy.format(fields="rowid", now=now)
            )
            await self.execute(delete, (cull_limit,), sql=sql)

            for (filename,) in rows:
                cleanup(filename)

    async def touch(self, key, expire=None, retry=False):
        """Touch `key` in cache and update `expire` time.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param float expire: seconds until item expires
            (default None, no expiry)
        :param bool retry: retry if database timeout occurs (default False)
        :return: True if key was touched
        :raises Timeout: if database timeout occurs

        """
        now = time.time()
        db_key, raw = self._disk.put(key)
        expire_time = None if expire is None else now + expire

        async with self._transact(retry) as (sql, _):
            rows = await self.fetchall(
                "SELECT rowid, expire_time FROM Cache WHERE key = ? AND raw = ?",
                (db_key, raw),
                sql=sql,
            )

            if rows:
                (rowid, old_expire_time), = rows

                if old_expire_time is None or old_expire_time > now:
                    await self.execute(
                        "UPDATE Cache SET expire_time = ? WHERE rowid = ?",
                        (expire_time, rowid),
                        sql=sql,
                    )
                    return True

        return False

    async def add(self, key, value, expire=None, read=False, tag=None, retry=False):
        """Add `key` and `value` item to cache.

        Similar to `set`, but only add to cache if key not present.

        Operation is atomic. Only one concurrent add operation for a given key
        will succeed.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param value: value for item
        :param float expire: seconds until the key expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param str tag: text to associate with key (default None)
        :param bool retry: retry if database timeout occurs (default False)
        :return: True if item was added
        :raises Timeout: if database timeout occurs

        """
        now = time.time()
        db_key, raw = self._disk.put(key)
        expire_time = None if expire is None else now + expire
        size, mode, filename, db_value = await self._disk.store(value, read, key=key)
        columns = (expire_time, tag, size, mode, filename, db_value)

        async with self._transact(retry, filename) as (sql, cleanup):
            rows = await self.fetchall(
                "SELECT rowid, filename, expire_time FROM Cache"
                " WHERE key = ? AND raw = ?",
                (db_key, raw),
                sql=sql,
            )

            if rows:
                (rowid, old_filename, old_expire_time), = rows

                if old_expire_time is None or old_expire_time > now:
                    cleanup(filename)
                    return False

                cleanup(old_filename)
                await self._row_update(rowid, now, columns)
            else:
                await self._row_insert(db_key, raw, now, columns)

            await self._cull(now, sql, cleanup)

            return True

    async def incr(self, key, delta=1, default=0, retry=False):
        """Increment value by delta for item with key.

        If key is missing and default is None then raise KeyError. Else if key
        is missing and default is not None then use default for value.

        Operation is atomic. All concurrent increment operations will be
        counted individually.

        Assumes value may be stored in a SQLite column. Most builds that target
        machines with 64-bit pointer widths will support 64-bit signed
        integers.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param int delta: amount to increment (default 1)
        :param int default: value if key is missing (default 0)
        :param bool retry: retry if database timeout occurs (default False)
        :return: new value for item
        :raises KeyError: if key is not found and default is None
        :raises Timeout: if database timeout occurs

        """
        now = time.time()
        db_key, raw = self._disk.put(key)
        select = (
            "SELECT rowid, expire_time, filename, value FROM Cache"
            " WHERE key = ? AND raw = ?"
        )

        async with self._transact(retry) as (sql, cleanup):
            rows = await self.fetchall(select, (db_key, raw), sql=sql)

            if not rows:
                if default is None:
                    raise KeyError(key)

                value = default + delta
                columns = (None, None) + await self._disk.store(value, False, key=key)
                await self._row_insert(db_key, raw, now, columns, sql=sql)
                await self._cull(now, sql, cleanup)
                return value

            (rowid, expire_time, filename, value), = rows

            if expire_time is not None and expire_time < now:
                if default is None:
                    raise KeyError(key)

                value = default + delta
                columns = (None, None) + await self._disk.store(value, False, key=key)
                await self._row_update(rowid, now, columns, sql=sql)
                await self._cull(now, sql, cleanup)
                cleanup(filename)
                return value

            value += delta

            columns = "store_time = ?, value = ?"
            update_column = EVICTION_POLICY[self.eviction_policy]["get"]

            if update_column is not None:
                columns += ", " + update_column.format(now=now)

            update = "UPDATE Cache SET %s WHERE rowid = ?" % columns
            await self.execute(update, (now, value, rowid), sql=sql)

            return value

    async def decr(self, key, delta=1, default=0, retry=False):
        """Decrement value by delta for item with key.

        If key is missing and default is None then raise KeyError. Else if key
        is missing and default is not None then use default for value.

        Operation is atomic. All concurrent decrement operations will be
        counted individually.

        Unlike Memcached, negative values are supported. Value may be
        decremented below zero.

        Assumes value may be stored in a SQLite column. Most builds that target
        machines with 64-bit pointer widths will support 64-bit signed
        integers.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param int delta: amount to decrement (default 1)
        :param int default: value if key is missing (default 0)
        :param bool retry: retry if database timeout occurs (default False)
        :return: new value for item
        :raises KeyError: if key is not found and default is None
        :raises Timeout: if database timeout occurs

        """
        return await self.incr(key, -delta, default, retry)

    async def size(self, key):
        db_key, raw = self._disk.put(key)
        select = (
            "SELECT mode, filename, length(value)"
            " FROM Cache WHERE key = ? AND raw = ?"
            " AND (expire_time IS NULL OR expire_time > ?)"
        )
        row = await self.fetchone(select, (db_key, raw, time.time()))
        if not row:
            return None

        mode, filename, size = row
        if mode == MODE_BINARY:
            try:
                full_path = op.join(self._directory, filename)
                size = op.getsize(full_path)
            except Exception:
                pass

        return size

    async def get(
        self, key, default=None, read=False, expire_time=False, tag=False, retry=False
    ):
        """Retrieve value from cache. If `key` is missing, return `default`.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param default: value to return if key is missing (default None)
        :param bool read: if True, return file handle to value
            (default False)
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :param bool retry: retry if database timeout occurs (default False)
        :return: value for item or default if key not found
        :raises Timeout: if database timeout occurs

        """
        db_key, raw = self._disk.put(key)
        update_column = EVICTION_POLICY[self.eviction_policy]["get"]
        select = (
            "SELECT rowid, expire_time, tag, mode, filename, value"
            " FROM Cache WHERE key = ? AND raw = ?"
            " AND (expire_time IS NULL OR expire_time > ?)"
        )

        if expire_time and tag:
            default = (default, None, None)
        elif expire_time or tag:
            default = (default, None)

        if not self.statistics and update_column is None:
            # Fast path, no transaction necessary.

            row = await self.fetchone(select, (db_key, raw, time.time()))

            if not row:
                return default

            rowid, db_expire_time, db_tag, mode, filename, db_value = row

            try:
                value = await self._disk.fetch(mode, filename, db_value, read)
            except IOError:
                # Key was deleted before we could retrieve result.
                return default

        else:  # Slow path, transaction required.
            cache_hit = 'UPDATE Settings SET value = value + 1 WHERE key = "hits"'
            cache_miss = 'UPDATE Settings SET value = value + 1 WHERE key = "misses"'

            async with self._transact(retry) as (sql, _):
                row = await self.fetchone(select, (db_key, raw, time.time()), sql=sql)

                if not row:
                    if self.statistics:
                        await self.execute(cache_miss, sql=sql)
                    return default

                rowid, db_expire_time, db_tag, mode, filename, db_value = row

                try:
                    value = await self._disk.fetch(mode, filename, db_value, read)
                except IOError as error:
                    if error.errno == errno.ENOENT:
                        # Key was deleted before we could retrieve result.
                        if self.statistics:
                            await self.execute(cache_miss, sql=sql)
                        return default
                    else:
                        raise

                if self.statistics:
                    await self.execute(cache_hit, sql=sql)

                now = time.time()
                update = "UPDATE Cache SET %s WHERE rowid = ?"

                if update_column is not None:
                    await self.execute(
                        update % update_column.format(now=now), (rowid,), sql=sql
                    )

        if expire_time and tag:
            return (value, db_expire_time, db_tag)
        elif expire_time:
            return (value, db_expire_time)
        elif tag:
            return (value, db_tag)
        else:
            return value

    async def read(self, key, retry=False):
        """Return file handle value corresponding to `key` from cache.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key matching item
        :param bool retry: retry if database timeout occurs (default False)
        :return: file open for reading in binary mode
        :raises KeyError: if key is not found
        :raises Timeout: if database timeout occurs

        """
        handle = await self.get(key, default=ENOVAL, read=True, retry=retry)
        if handle is ENOVAL:
            raise KeyError(key)
        return handle

    async def contains(self, key):
        """Return `True` if `key` matching item is found in cache.

        :param key: key matching item
        :return: True if key matching item

        """
        db_key, raw = self._disk.put(key)
        select = (
            "SELECT rowid FROM Cache"
            " WHERE key = ? AND raw = ?"
            " AND (expire_time IS NULL OR expire_time > ?)"
        )

        rows = await self.fetchall(select, (db_key, raw, time.time()))

        return bool(rows)

    async def pop(self, key, default=None, expire_time=False, tag=False, retry=False):
        """Remove corresponding item for `key` from cache and return value.

        If `key` is missing, return `default`.

        Operation is atomic. Concurrent operations will be serialized.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key for item
        :param default: value to return if key is missing (default None)
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :param bool retry: retry if database timeout occurs (default False)
        :return: value for item or default if key not found
        :raises Timeout: if database timeout occurs

        """
        db_key, raw = self._disk.put(key)
        select = (
            "SELECT rowid, expire_time, tag, mode, filename, value"
            " FROM Cache WHERE key = ? AND raw = ?"
            " AND (expire_time IS NULL OR expire_time > ?)"
        )

        if expire_time and tag:
            default = default, None, None
        elif expire_time or tag:
            default = default, None

        async with self._transact(retry) as (sql, _):
            rows = await self.fetchall(select, (db_key, raw, time.time()), sql=sql)

            if not rows:
                return default

            (rowid, db_expire_time, db_tag, mode, filename, db_value), = rows

            await self.execute("DELETE FROM Cache WHERE rowid = ?", (rowid,), sql=sql)

        try:
            value = await self._disk.fetch(mode, filename, db_value, False)
        except IOError as error:
            if error.errno == errno.ENOENT:
                # Key was deleted before we could retrieve result.
                return default
            else:
                raise
        finally:
            if filename is not None:
                self._disk.remove(filename)

        if expire_time and tag:
            return value, db_expire_time, db_tag
        elif expire_time:
            return value, db_expire_time
        elif tag:
            return value, db_tag
        else:
            return value

    async def delete(self, key, retry=False):
        """Delete corresponding item for `key` from cache.

        Missing keys are ignored.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param key: key matching item
        :param bool retry: retry if database timeout occurs (default False)
        :return: True if item was deleted
        :raises Timeout: if database timeout occurs

        """
        try:
            db_key, raw = self._disk.put(key)

            async with self._transact(retry) as (sql, cleanup):
                rows = await self.fetchall(
                    "SELECT rowid, filename FROM Cache"
                    " WHERE key = ? AND raw = ?"
                    " AND (expire_time IS NULL OR expire_time > ?)",
                    (db_key, raw, time.time()),
                    sql=sql,
                )

                if not rows:
                    raise KeyError(key)

                (rowid, filename), = rows
                await self.execute(
                    "DELETE FROM Cache WHERE rowid = ?", (rowid,), sql=sql
                )
                cleanup(filename)

                return True

        except KeyError:
            return False

    async def push(
        self,
        value,
        prefix=None,
        side="back",
        expire=None,
        read=False,
        tag=None,
        retry=False,
    ):
        """Push `value` onto `side` of queue identified by `prefix` in cache.

        When prefix is None, integer keys are used. Otherwise, string keys are
        used in the format "prefix-integer". Integer starts at 500 trillion.

        Defaults to pushing value on back of queue. Set side to 'front' to push
        value on front of queue. Side must be one of 'back' or 'front'.

        Operation is atomic. Concurrent operations will be serialized.

        When `read` is `True`, `value` should be a file-like object opened
        for reading in binary mode.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        See also `Cache.pull`.

        >>> cache = Cache('/tmp/diskcache')
        >>> _ = cache.clear()
        >>> print(cache.push('first value'))
        500000000000000
        >>> cache.get(500000000000000)
        'first value'
        >>> print(cache.push('second value'))
        500000000000001
        >>> print(cache.push('third value', side='front'))
        499999999999999
        >>> cache.push(1234, prefix='userids')
        'userids-500000000000000'

        :param value: value for item
        :param str prefix: key prefix (default None, key is integer)
        :param str side: either 'back' or 'front' (default 'back')
        :param float expire: seconds until the key expires
            (default None, no expiry)
        :param bool read: read value as bytes from file (default False)
        :param str tag: text to associate with key (default None)
        :param bool retry: retry if database timeout occurs (default False)
        :return: key for item in cache
        :raises Timeout: if database timeout occurs

        """
        if prefix is None:
            min_key = 0
            max_key = 999999999999999
        else:
            min_key = prefix + "-000000000000000"
            max_key = prefix + "-999999999999999"

        now = time.time()
        raw = True
        expire_time = None if expire is None else now + expire
        size, mode, filename, db_value = await self._disk.store(value, read)
        columns = (expire_time, tag, size, mode, filename, db_value)
        order = {"back": "DESC", "front": "ASC"}
        select = (
            "SELECT key FROM Cache"
            " WHERE ? < key AND key < ? AND raw = ?"
            " ORDER BY key %s LIMIT 1"
        ) % order[side]

        async with self._transact(retry, filename) as (sql, cleanup):
            rows = await self.fetchall(select, (min_key, max_key, raw), sql=sql)

            if rows:
                (key,), = rows

                if prefix is not None:
                    num = int(key[(key.rfind("-") + 1) :])
                else:
                    num = key

                if side == "back":
                    num += 1
                else:
                    assert side == "front"
                    num -= 1
            else:
                num = 500000000000000

            if prefix is not None:
                db_key = "{0}-{1:015d}".format(prefix, num)
            else:
                db_key = num

            await self._row_insert(db_key, raw, now, columns)
            await self._cull(now, sql, cleanup)

            return db_key

    async def pull(
        self,
        prefix=None,
        default=(None, None),
        side="front",
        expire_time=False,
        tag=False,
        retry=False,
    ):
        """Pull key and value item pair from `side` of queue in cache.

        When prefix is None, integer keys are used. Otherwise, string keys are
        used in the format "prefix-integer". Integer starts at 500 trillion.

        If queue is empty, return default.

        Defaults to pulling key and value item pairs from front of queue. Set
        side to 'back' to pull from back of queue. Side must be one of 'front'
        or 'back'.

        Operation is atomic. Concurrent operations will be serialized.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        See also `Cache.push` and `Cache.get`.

        >>> cache = Cache('/tmp/diskcache')
        >>> _ = cache.clear()
        >>> cache.pull()
        (None, None)
        >>> for letter in 'abc':
        ...     print(cache.push(letter))
        500000000000000
        500000000000001
        500000000000002
        >>> key, value = cache.pull()
        >>> print(key)
        500000000000000
        >>> value
        'a'
        >>> _, value = cache.pull(side='back')
        >>> value
        'c'
        >>> cache.push(1234, 'userids')
        'userids-500000000000000'
        >>> _, value = cache.pull('userids')
        >>> value
        1234

        :param str prefix: key prefix (default None, key is integer)
        :param default: value to return if key is missing
            (default (None, None))
        :param str side: either 'front' or 'back' (default 'front')
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :param bool retry: retry if database timeout occurs (default False)
        :return: key and value item pair or default if queue is empty
        :raises Timeout: if database timeout occurs

        """
        # Caution: Nearly identical code exists in Cache.peek
        if prefix is None:
            min_key = 0
            max_key = 999999999999999
        else:
            min_key = prefix + "-000000000000000"
            max_key = prefix + "-999999999999999"

        order = {"front": "ASC", "back": "DESC"}
        select = (
            "SELECT rowid, key, expire_time, tag, mode, filename, value"
            " FROM Cache WHERE ? < key AND key < ? AND raw = 1"
            " ORDER BY key %s LIMIT 1"
        ) % order[side]

        if expire_time and tag:
            default = default, None, None
        elif expire_time or tag:
            default = default, None

        while True:
            while True:
                async with self._transact(retry) as (sql, cleanup):
                    rows = await self.fetchall(select, (min_key, max_key), sql=sql)

                    if not rows:
                        return default

                    (rowid, key, db_expire, db_tag, mode, name, db_value), = rows

                    await self.execute(
                        "DELETE FROM Cache WHERE rowid = ?", (rowid,), sql=sql
                    )

                    if db_expire is not None and db_expire < time.time():
                        cleanup(name)
                    else:
                        break

            try:
                value = await self._disk.fetch(mode, name, db_value, False)
            except IOError as error:
                if error.errno == errno.ENOENT:
                    # Key was deleted before we could retrieve result.
                    continue
                else:
                    raise
            finally:
                if name is not None:
                    self._disk.remove(name)
            break

        if expire_time and tag:
            return (key, value), db_expire, db_tag
        elif expire_time:
            return (key, value), db_expire
        elif tag:
            return (key, value), db_tag
        else:
            return key, value

    async def peek(
        self,
        prefix=None,
        default=(None, None),
        side="front",
        expire_time=False,
        tag=False,
        retry=False,
    ):
        """Peek at key and value item pair from `side` of queue in cache.

        When prefix is None, integer keys are used. Otherwise, string keys are
        used in the format "prefix-integer". Integer starts at 500 trillion.

        If queue is empty, return default.

        Defaults to peeking at key and value item pairs from front of queue.
        Set side to 'back' to pull from back of queue. Side must be one of
        'front' or 'back'.

        Expired items are deleted from cache. Operation is atomic. Concurrent
        operations will be serialized.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        See also `Cache.pull` and `Cache.push`.

        >>> cache = Cache('/tmp/diskcache')
        >>> _ = cache.clear()
        >>> for letter in 'abc':
        ...     print(cache.push(letter))
        500000000000000
        500000000000001
        500000000000002
        >>> key, value = cache.peek()
        >>> print(key)
        500000000000000
        >>> value
        'a'
        >>> key, value = cache.peek(side='back')
        >>> print(key)
        500000000000002
        >>> value
        'c'

        :param str prefix: key prefix (default None, key is integer)
        :param default: value to return if key is missing
            (default (None, None))
        :param str side: either 'front' or 'back' (default 'front')
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :param bool retry: retry if database timeout occurs (default False)
        :return: key and value item pair or default if queue is empty
        :raises Timeout: if database timeout occurs

        """
        # Caution: Nearly identical code exists in Cache.pull
        if prefix is None:
            min_key = 0
            max_key = 999999999999999
        else:
            min_key = prefix + "-000000000000000"
            max_key = prefix + "-999999999999999"

        order = {"front": "ASC", "back": "DESC"}
        select = (
            "SELECT rowid, key, expire_time, tag, mode, filename, value"
            " FROM Cache WHERE ? < key AND key < ? AND raw = 1"
            " ORDER BY key %s LIMIT 1"
        ) % order[side]

        if expire_time and tag:
            default = default, None, None
        elif expire_time or tag:
            default = default, None

        while True:
            while True:
                async with self._transact(retry) as (sql, cleanup):
                    rows = await self.fetchall(select, (min_key, max_key), sql=sql)

                    if not rows:
                        return default

                    (rowid, key, db_expire, db_tag, mode, name, db_value), = rows

                    if db_expire is not None and db_expire < time.time():
                        await self.execute(
                            "DELETE FROM Cache WHERE rowid = ?", (rowid,), sql=sql
                        )
                        cleanup(name)
                    else:
                        break

            try:
                value = await self._disk.fetch(mode, name, db_value, False)
            except IOError as error:
                if error.errno == errno.ENOENT:
                    # Key was deleted before we could retrieve result.
                    continue
                else:
                    raise
            finally:
                if name is not None:
                    self._disk.remove(name)
            break

        if expire_time and tag:
            return (key, value), db_expire, db_tag
        elif expire_time:
            return (key, value), db_expire
        elif tag:
            return (key, value), db_tag
        else:
            return key, value

    async def peekitem(self, last=True, expire_time=False, tag=False, retry=False):
        """Peek at key and value item pair in cache based on iteration order.

        Expired items are deleted from cache. Operation is atomic. Concurrent
        operations will be serialized.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        >>> cache = Cache('/tmp/diskcache')
        >>> _ = cache.clear()
        >>> for num, letter in enumerate('abc'):
        ...     cache[letter] = num
        >>> cache.peekitem()
        ('c', 2)
        >>> cache.peekitem(last=False)
        ('a', 0)

        :param bool last: last item in iteration order (default True)
        :param bool expire_time: if True, return expire_time in tuple
            (default False)
        :param bool tag: if True, return tag in tuple (default False)
        :param bool retry: retry if database timeout occurs (default False)
        :return: key and value item pair
        :raises KeyError: if cache is empty
        :raises Timeout: if database timeout occurs

        """
        order = ("ASC", "DESC")
        select = (
            "SELECT rowid, key, raw, expire_time, tag, mode, filename, value"
            " FROM Cache ORDER BY rowid %s LIMIT 1"
        ) % order[last]

        while True:
            while True:
                async with self._transact(retry) as (sql, cleanup):
                    rows = await self.fetchall(select, sql=sql)

                    if not rows:
                        raise KeyError("dictionary is empty")

                    (
                        rowid,
                        db_key,
                        raw,
                        db_expire,
                        db_tag,
                        mode,
                        name,
                        db_value,
                    ), = rows

                    if db_expire is not None and db_expire < time.time():
                        await self.execute(
                            "DELETE FROM Cache WHERE rowid = ?", (rowid,), sql=sql
                        )
                        cleanup(name)
                    else:
                        break

            key = self._disk.get(db_key, raw)

            try:
                value = await self._disk.fetch(mode, name, db_value, False)
            except IOError as error:
                if error.errno == errno.ENOENT:
                    # Key was deleted before we could retrieve result.
                    continue
                else:
                    raise
            finally:
                if name is not None:
                    self._disk.remove(name)
            break

        if expire_time and tag:
            return (key, value), db_expire, db_tag
        elif expire_time:
            return (key, value), db_expire
        elif tag:
            return (key, value), db_tag
        else:
            return key, value

    memoize = memoize

    async def check(self, fix=False, retry=False):
        """Check database and file system consistency.

        Intended for use in testing and post-mortem error analysis.

        While checking the Cache table for consistency, a writer lock is held
        on the database. The lock blocks other cache clients from writing to
        the database. For caches with many file references, the lock may be
        held for a long time. For example, local benchmarking shows that a
        cache with 1,000 file references takes ~60ms to check.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param bool fix: correct inconsistencies
        :param bool retry: retry if database timeout occurs (default False)
        :return: list of warnings
        :raises Timeout: if database timeout occurs

        """
        # pylint: disable=access-member-before-definition,W0201
        with warnings.catch_warnings(record=True) as warns:
            # Check integrity of database.

            rows = await self.fetchall("PRAGMA integrity_check")

            if len(rows) != 1 or rows[0][0] != u"ok":
                for (message,) in rows:
                    warnings.warn(message)

            if fix:
                await self.execute("VACUUM")

            async with self._transact(retry) as (sql, _):

                # Check Cache.filename against file system.

                filenames = set()
                select = (
                    "SELECT rowid, size, filename FROM Cache"
                    " WHERE filename IS NOT NULL"
                )

                rows = await self.fetchall(select, sql=sql)

                for rowid, size, filename in rows:
                    full_path = op.join(self._directory, filename)
                    filenames.add(full_path)

                    if op.exists(full_path):
                        real_size = op.getsize(full_path)

                        if size != real_size:
                            message = "wrong file size: %s, %d != %d"
                            args = full_path, real_size, size
                            warnings.warn(message % args)

                            if fix:
                                await self.execute(
                                    "UPDATE Cache SET size = ? WHERE rowid = ?",
                                    (real_size, rowid),
                                    sql=sql,
                                )

                        continue

                    warnings.warn("file not found: %s" % full_path)

                    if fix:
                        await self.execute(
                            "DELETE FROM Cache WHERE rowid = ?", (rowid,), sql=sql
                        )

                # Check file system against Cache.filename.

                for dirpath, _, files in os.walk(self._directory):
                    paths = [op.join(dirpath, filename) for filename in files]
                    error = set(paths) - filenames

                    for full_path in error:
                        if DBNAME in full_path:
                            continue

                        message = "unknown file: %s" % full_path
                        warnings.warn(message, UnknownFileWarning)

                        if fix:
                            os.remove(full_path)

                # Check for empty directories.

                for dirpath, dirs, files in os.walk(self._directory):
                    if not (dirs or files):
                        message = "empty directory: %s" % dirpath
                        warnings.warn(message, EmptyDirWarning)

                        if fix:
                            os.rmdir(dirpath)

                # Check Settings.count against count of Cache rows.

                await self.reset("count")
                (count,), = await self.fetchall("SELECT COUNT(key) FROM Cache", sql=sql)

                if self.count != count:
                    message = "Settings.count != COUNT(Cache.key); %d != %d"
                    warnings.warn(message % (self.count, count))

                    if fix:
                        await self.execute(
                            "UPDATE Settings SET value = ? WHERE key = ?",
                            (count, "count"),
                            sql=sql,
                        )

                # Check Settings.size against sum of Cache.size column.

                await self.reset("size")
                select_size = "SELECT COALESCE(SUM(size), 0) FROM Cache"
                (size,), = await self.fetchall(select_size, sql=sql)

                if self.size != size:
                    message = "Settings.size != SUM(Cache.size); %d != %d"
                    warnings.warn(message % (self.size, size))

                    if fix:
                        await self.execute(
                            "UPDATE Settings SET value = ? WHERE key =?",
                            (size, "size"),
                            sql=sql,
                        )

            return warns

    async def create_tag_index(self):
        """Create tag index on cache database.

        It is better to initialize cache with `tag_index=True` than use this.

        :raises Timeout: if database timeout occurs

        """
        await self.execute(
            "CREATE INDEX IF NOT EXISTS Cache_tag_rowid ON Cache(tag, rowid)"
        )
        await self.reset("tag_index", 1)

    async def drop_tag_index(self):
        """Drop tag index on cache database.

        :raises Timeout: if database timeout occurs

        """
        await self.execute("DROP INDEX IF EXISTS Cache_tag_rowid")
        await self.reset("tag_index", 0)

    async def evict(self, tag, retry=False):
        """Remove items with matching `tag` from cache.

        Removing items is an iterative process. In each iteration, a subset of
        items is removed. Concurrent writes may occur between iterations.

        If a :exc:`Timeout` occurs, the first element of the exception's
        `args` attribute will be the number of items removed before the
        exception occurred.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param str tag: tag identifying items
        :param bool retry: retry if database timeout occurs (default False)
        :return: count of rows removed
        :raises Timeout: if database timeout occurs

        """
        select = (
            "SELECT rowid, filename FROM Cache"
            " WHERE tag = ? AND rowid > ?"
            " ORDER BY rowid LIMIT ?"
        )
        args = [tag, 0, 100]
        return await self._select_delete(select, args, arg_index=1, retry=retry)

    async def expire(self, now=None, retry=False):
        """Remove expired items from cache.

        Removing items is an iterative process. In each iteration, a subset of
        items is removed. Concurrent writes may occur between iterations.

        If a :exc:`Timeout` occurs, the first element of the exception's
        `args` attribute will be the number of items removed before the
        exception occurred.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param float now: current time (default None, ``time.time()`` used)
        :param bool retry: retry if database timeout occurs (default False)
        :return: count of items removed
        :raises Timeout: if database timeout occurs

        """
        select = (
            "SELECT rowid, expire_time, filename FROM Cache"
            " WHERE ? < expire_time AND expire_time < ?"
            " ORDER BY expire_time LIMIT ?"
        )
        args = [0, now or time.time(), 100]
        return await self._select_delete(select, args, row_index=1, retry=retry)

    async def cull(self, retry=False):
        """Cull items from cache until volume is less than size limit.

        Removing items is an iterative process. In each iteration, a subset of
        items is removed. Concurrent writes may occur between iterations.

        If a :exc:`Timeout` occurs, the first element of the exception's
        `args` attribute will be the number of items removed before the
        exception occurred.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param bool retry: retry if database timeout occurs (default False)
        :return: count of items removed
        :raises Timeout: if database timeout occurs

        """
        now = time.time()

        # Remove expired items.

        count = self.expire(now)

        # Remove items by policy.

        select_policy = EVICTION_POLICY[self.eviction_policy]["cull"]

        if select_policy is None:
            return

        select_filename = select_policy.format(fields="filename", now=now)

        try:
            while (await self.volume()) > self.size_limit:
                async with self._transact(retry) as (sql, cleanup):
                    rows = await self.fetchall(select_filename, (10,), sql=sql)

                    if not rows:
                        break

                    count += len(rows)
                    delete = (
                        "DELETE FROM Cache WHERE rowid IN (%s)"
                        % select_policy.format(fields="rowid", now=now)
                    )
                    await self.execute(delete, (10,), sql=sql)

                    for (filename,) in rows:
                        cleanup(filename)
        except Timeout:
            raise Timeout(count)

        return count

    async def clear(self, retry=False):
        """Remove all items from cache.

        Removing items is an iterative process. In each iteration, a subset of
        items is removed. Concurrent writes may occur between iterations.

        If a :exc:`Timeout` occurs, the first element of the exception's
        `args` attribute will be the number of items removed before the
        exception occurred.

        Raises :exc:`Timeout` error when database timeout occurs and `retry` is
        `False` (default).

        :param bool retry: retry if database timeout occurs (default False)
        :return: count of rows removed
        :raises Timeout: if database timeout occurs

        """
        select = (
            "SELECT rowid, filename FROM Cache"
            " WHERE rowid > ?"
            " ORDER BY rowid LIMIT ?"
        )
        args = [0, 100]
        return await self._select_delete(select, args, retry=retry)

    async def _select_delete(self, select, args, row_index=0, arg_index=0, retry=False):
        count = 0
        delete = "DELETE FROM Cache WHERE rowid IN (%s)"

        try:
            while True:
                async with self._transact(retry) as (sql, cleanup):
                    rows = await self.fetchall(select, args, sql=sql)

                    if not rows:
                        break

                    count += len(rows)
                    await self.execute(
                        delete % ",".join(str(row[0]) for row in rows), sql=sql
                    )

                    for row in rows:
                        args[arg_index] = row[row_index]
                        cleanup(row[-1])

        except Timeout:
            raise Timeout(count)

        return count

    async def iterkeys(self, reverse=False):
        """Iterate Cache keys in database sort order.

        >>> cache = Cache('/tmp/diskcache')
        >>> _ = cache.clear()
        >>> for key in [4, 1, 3, 0, 2]:
        ...     cache[key] = key
        >>> list(cache.iterkeys())
        [0, 1, 2, 3, 4]
        >>> list(cache.iterkeys(reverse=True))
        [4, 3, 2, 1, 0]

        :param bool reverse: reverse sort order (default False)
        :return: iterator of Cache keys

        """
        limit = 100
        _disk_get = self._disk.get

        if reverse:
            select = "SELECT key, raw FROM Cache ORDER BY key DESC, raw DESC LIMIT 1"
            iterate = (
                "SELECT key, raw FROM Cache"
                " WHERE key = ? AND raw < ? OR key < ?"
                " ORDER BY key DESC, raw DESC LIMIT ?"
            )
        else:
            select = "SELECT key, raw FROM Cache ORDER BY key ASC, raw ASC LIMIT 1"
            iterate = (
                "SELECT key, raw FROM Cache"
                " WHERE key = ? AND raw > ? OR key > ?"
                " ORDER BY key ASC, raw ASC LIMIT ?"
            )

        row = await self.fetchall(select)

        if row:
            (key, raw), = row
        else:
            return

        yield _disk_get(key, raw)

        while True:
            rows = await self.fetchall(iterate, (key, raw, key, limit))

            if not rows:
                break

            for key, raw in rows:
                yield _disk_get(key, raw)

    async def _iter(self, ascending=True):
        rows = await self.fetchall("SELECT MAX(rowid) FROM Cache")
        (max_rowid,), = rows
        yield  # Signal ready.

        if max_rowid is None:
            return

        bound = max_rowid + 1
        limit = 100
        _disk_get = self._disk.get
        rowid = 0 if ascending else bound
        select = (
            "SELECT rowid, key, raw FROM Cache"
            " WHERE ? < rowid AND rowid < ?"
            " ORDER BY rowid %s LIMIT ?"
        ) % ("ASC" if ascending else "DESC")

        while True:
            if ascending:
                args = (rowid, bound, limit)
            else:
                args = (0, rowid, limit)

            rows = await self.fetchall(select, args)

            if not rows:
                break

            for rowid, key, raw in rows:
                yield _disk_get(key, raw)

    async def __aiter__(self):
        "Iterate keys in cache including expired items."
        iterator = await self._iter()
        await iterator.__anext__()
        return iterator

    async def __reversed__(self):
        "Reverse iterate keys in cache including expired items."
        iterator = await self._iter(ascending=False)
        await iterator.__anext__()
        return iterator

    async def stats(self, enable=True, reset=False):
        """Return cache statistics hits and misses.

        :param bool enable: enable collecting statistics (default True)
        :param bool reset: reset hits and misses to 0 (default False)
        :return: (hits, misses)

        """
        # pylint: disable=E0203,W0201
        result = (await self.reset("hits"), await self.reset("misses"))

        if reset:
            await self.reset("hits", 0)
            await self.reset("misses", 0)

        await self.reset("statistics", enable)

        return result

    async def volume(self):
        """Return estimated total size of cache on disk.

        :return: size in bytes

        """
        (page_count,), = await self.fetchall("PRAGMA page_count")
        total_size = self._page_size * page_count + await self.reset("size")
        return total_size

    async def close(self):
        """Close database connection.

        """
        con = getattr(self._local, "con", None)

        if con is None:
            return

        await con.close()

        try:
            delattr(self._local, "con")
        except AttributeError:
            pass

    async def __aenter__(self):
        if not self.initialized:
            await self.init()
        return self

    async def __aexit__(self, *exception):
        await self.close()

    async def __len__(self):
        "Count of items in cache including expired items."
        return await self.reset("count")

    def __getstate__(self):
        return (self.directory, self.timeout, type(self.disk))

    def __setstate__(self, state):
        self.__init__(*state)

    async def reset(self, key, value=ENOVAL, update=True):
        """Reset `key` and `value` item from Settings table.

        Use `reset` to update the value of Cache settings correctly. Cache
        settings are stored in the Settings table of the SQLite database. If
        `update` is ``False`` then no attempt is made to update the database.

        If `value` is not given, it is reloaded from the Settings
        table. Otherwise, the Settings table is updated.

        Settings with the ``disk_`` prefix correspond to Disk
        attributes. Updating the value will change the unprefixed attribute on
        the associated Disk instance.

        Settings with the ``sqlite_`` prefix correspond to SQLite
        pragmas. Updating the value will execute the corresponding PRAGMA
        statement.

        SQLite PRAGMA statements may be executed before the Settings table
        exists in the database by setting `update` to ``False``.

        :param str key: Settings key for item
        :param value: value for item (optional)
        :param bool update: update database Settings table (default True)
        :return: updated value for item
        :raises Timeout: if database timeout occurs

        """

        if value is ENOVAL:
            select = "SELECT value FROM Settings WHERE key = ?"
            (value,), = await self.fetchall(select, (key,), retry=True)
            setattr(self, key, value)
            return value

        if update:
            statement = "UPDATE Settings SET value = ? WHERE key = ?"
            await self.execute(statement, (value, key), retry=True)

        if key.startswith("sqlite_"):
            pragma = key[7:]

            # 2016-02-17 GrantJ - PRAGMA and isolation_level=None
            # don't always play nicely together. Retry setting the
            # PRAGMA. I think some PRAGMA statements expect to
            # immediately take an EXCLUSIVE lock on the database. I
            # can't find any documentation for this but without the
            # retry, stress will intermittently fail with multiple
            # processes.

            # 2018-11-05 GrantJ - Avoid setting pragma values that
            # are already set. Pragma settings like auto_vacuum and
            # journal_mode can take a long time or may not work after
            # tables have been created.

            start = time.time()
            while True:
                try:
                    try:
                        (old_value,), = await self.fetchall("PRAGMA %s" % (pragma))
                        update = old_value != value
                    except ValueError:
                        update = True
                    if update:
                        await self.fetchall("PRAGMA %s = %s" % (pragma, value))
                    break
                except sqlite3.OperationalError as exc:
                    if str(exc) != "database is locked":
                        raise
                    diff = time.time() - start
                    if diff > 60:
                        raise
                    await asyncio.sleep(0.001)
        elif key.startswith("disk_"):
            attr = key[5:]
            setattr(self._disk, attr, value)

        setattr(self, key, value)
        return value
