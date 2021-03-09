from contextlib import contextmanager
from dataclasses import dataclass
from typing import List, Optional, Tuple, Any, Iterable, Dict
import pyodbc
import time
from dataclasses import dataclass
from typing import Optional

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.helper_types import Port


@dataclass
class TeradataCredentials(Credentials):
    host: str
    username: str
    password: str
    driver: str
    port: Optional[Port] = 5480
    dsn: Optional[str] = None

    _ALIASES = {
        'user': 'username',
        'pass': 'password'
    }

    @property
    def type(self):
        return 'teradata'

    def _connection_keys(self):
        return ('dsn', 
                'driver', 
                'host', 
                'port', 
                'database', 
                'schema', 
                'username')


class TeradataConnectionManager(SQLConnectionManager):
    TYPE = 'teradata'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except pyodbc.DatabaseError as e:
            logger.error(f'pyodbc error: {str(e)}')
            try:
                self.release()
            except pyodbc.DatabaseError:
                logger.error('Failed to release connection!')

        except Exception as e:
            logger.error("Error running SQL: {}".format(sql))
            logger.error("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise dbt.exceptions.RuntimeException(str(e))

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            connection_args = {}
            if credentials.dsn:
                connection_args = {
                    'DSN': credentials.dsn
                }
            else:
                connection_args = {
                    'DRIVER': credentials.driver,
                    'DBCNAME': credentials.host,
                    'PORT': credentials.port,
                    'DATABASE': credentials.database,
                    'SCHEMA': credentials.schema,
                    #'SESSIONMODE': 'ANSI'
                }

            handle = pyodbc.connect(
                UID=credentials.username,
                PWD=credentials.password,
                autocommit=False,
                **connection_args
            )

            connection.state = 'open'
            connection.handle = handle
        except Exception as e:
            logger.error(f"Got an error when attempting to open a pyodbc "
                         "connection '{e}'")
            connection.handle = None
            connection.state = 'fail'
            raise dbt.exceptions.FailedToConnectException(str(e))
        return connection

    @classmethod
    def get_response(cls, cursor): # -> AdapterResponse:
        return 'ok'
        #rowcount = cursor.rowcount
        #if rowcount == -1:
        #    code = 'FAIL'
        #else:
        #    code = 'SUCCESS'

        #return AdapterResponse(
        #    _message="{} {}".format(code, cursor.rowcount),
        #    rows_affected=cursor.rowcount,
        #    code=code
        #)

    def cancel(self, connection):
        # TODO Implement
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            logger.debug(f'On {connection.name}: {sql}')
            pre = time.time()

            cursor = connection.handle.cursor()

            # Driver will fail if bindings are passed to function and not needed
            if bindings:
                cursor.execute(sql, bindings)
            else:
                cursor.execute(sql)

            logger.debug(
                f"SQL status: {self.get_response(cursor)} in {time.time() - pre:0.2f} seconds")

            return connection, cursor

    def add_begin_query(self):
        return self.add_query('BEGIN TRANSACTION', auto_begin=False)    
        
    def add_commit_query(self):
        return self.add_query('END TRANSACTION', auto_begin=False)