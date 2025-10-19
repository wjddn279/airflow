#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
import os
import time
import traceback
from datetime import datetime
import weakref

from sqlalchemy import event, exc

from airflow.configuration import conf
from airflow.utils.sqlalchemy import get_orm_mapper

log = logging.getLogger(__name__)

connection_ids = {}

def connection_gc_callback(connection_record_ref):
    print(f"[GC] connection_record이 GC되었습니다: {id(connection_record_ref)}")
    log.debug(f"[GC] connection_record garbage collected")

def setup_event_handlers(engine):
    """Setups event handlers."""
    from airflow.models import import_all_models

    event.listen(get_orm_mapper(), "before_configured", import_all_models, once=True)

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info["pid"] = os.getpid()

    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()

    # this ensures coherence in mysql when storing datetimes (not required for postgres)
    if engine.dialect.name == "mysql":

        @event.listens_for(engine, "connect")
        def set_mysql_timezone(dbapi_connection, connection_record):
            log.debug("mysql connectio connect os_id: " + str(os.getpid()))
            log.debug("connect connection_record")
            print(connection_record.info)

            cursor = dbapi_connection.cursor()
            cursor.execute("SET time_zone = '+00:00'")
            cursor.close()

            #connection_ids[connection_record] = os.getpid()
            log.debug(f"[connect] New DB connection established, id={os.getpid()}")
            print(dbapi_connection)
            print(connection_record)

            weakref.finalize(dbapi_connection,
                             lambda: print(f"{datetime.now().isoformat()} dbapi_connection finalized via weakref in os {os.getpid()}",
                                           ))
            weakref.finalize(connection_record, lambda: print(f"{datetime.now().isoformat()} connection_record finalized via weakref in os {os.getpid()}"))

        @event.listens_for(engine, "close")
        def on_close(dbapi_connection, connection_record):
            log.debug("mysql connectio close os_id: " + str(os.getpid()))
            log.debug("close connection_record")
            print(connection_record.info)
            print(dbapi_connection)

            cid = connection_ids.get(connection_record)
            log.debug(f"[checkin] Connection closed in, id={cid}")

    @event.listens_for(engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        log.debug(f"query executed in os_id: {os.getpid()}")
        print(f"query executed in os_id: {os.getpid()}")

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        log.debug("mysql connectio checkout os_id: " + str(os.getpid()))
        log.debug("checkout connection_record")
        print(connection_record.info)
        print(dbapi_connection)

        if connection_record.info["pid"] != pid:
            connection_record.connection = connection_proxy.connection = None
            log.debug("checkout and raise error")
            raise exc.DisconnectionError(
                f"Connection record belongs to pid {connection_record.info['pid']}, "
                f"attempting to check out in pid {pid}"
            )

        cid = connection_ids.get(connection_record)
        log.debug(f"[checkout] Connection checked out, id={cid}")

    @event.listens_for(engine, "checkin")
    def on_checkin(dbapi_connection, connection_record):
        """풀에 커넥션이 반환될 때 호출"""
        log.debug("mysql connectio checkin os_id: " + str(os.getpid()))
        log.debug("checkin connection_record")
        print(connection_record.info)
        print(dbapi_connection)
        cid = connection_ids.get(connection_record)
        log.debug(f"[checkin] Connection checked in, id={cid}")

    if conf.getboolean("debug", "sqlalchemy_stats", fallback=False):

        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault("query_start_time", []).append(time.perf_counter())

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            total = time.perf_counter() - conn.info["query_start_time"].pop()
            file_name = [
                f"'{f.name}':{f.filename}:{f.lineno}"
                for f in traceback.extract_stack()
                if "sqlalchemy" not in f.filename
            ][-1]
            stack = [f for f in traceback.extract_stack() if "sqlalchemy" not in f.filename]
            stack_info = ">".join([f"{f.filename.rpartition('/')[-1]}:{f.name}" for f in stack][-3:])
            conn.info.setdefault("query_start_time", []).append(time.monotonic())
            log.info(
                "@SQLALCHEMY %s |$ %s |$ %s |$  %s ",
                total,
                file_name,
                stack_info,
                statement.replace("\n", " "),
            )
