import sqlite3
import concurrent.futures
import threading

assert (sqlite3.sqlite_version_info > (3, 3))


class Database:
    def __init__(self, loop, uri: str):
        self.loop = loop
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=1,
                                                               initializer=self._connect_database,
                                                               initargs=(uri,))
        # self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1,
        #                                                       initializer=self._connect_database,
        #                                                       initargs=(uri,))

    @staticmethod
    def _connect_database(uri) -> None:
        db = sqlite3.connect(database=uri, uri=True, isolation_level=None)  # with autocommit
        db.executescript("PRAGMA journal_mode=WAL")  # better reads & writes concurrency
        current_thread = threading.current_thread()
        local_storage = current_thread.__dict__
        local_storage['db'] = db.cursor()

    @staticmethod
    def _execute(sql: str, params):
        current_thread = threading.current_thread()
        local_storage = current_thread.__dict__
        db: sqlite3.Cursor = local_storage['db']
        if params:
            cur = db.execute(sql, params)
        else:
            cur = db.execute(sql)
        result = cur.fetchall()
        return result

    async def execute(self, sql: str, params = None):
        result = await self.loop.run_in_executor(self.executor, self._execute, sql, params)
        return result

    @staticmethod
    def _executescript(sql: str):
        current_thread = threading.current_thread()
        local_storage = current_thread.__dict__
        db: sqlite3.Cursor = local_storage['db']
        cur = db.executescript(sql)
        result = cur.fetchall()
        return result

    async def executescript(self, sql: str):
        result = await self.loop.run_in_executor(self.executor, self._executescript, sql)
        return result

    @staticmethod
    def _in_transaction():
        current_thread = threading.current_thread()
        local_storage = current_thread.__dict__
        db: sqlite3.Cursor = local_storage['db']
        return db.connection.in_transaction

    async def in_transaction(self):
        result = await self.loop.run_in_executor(self.executor, self._in_transaction)
        return result

    @staticmethod
    def _close():
        current_thread = threading.current_thread()
        local_storage = current_thread.__dict__
        db: sqlite3.Cursor = local_storage['db']
        return db.connection.close()

    async def close(self):
        result = await self.loop.run_in_executor(self.executor, self._close)
        return result

