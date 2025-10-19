import logging
import os
import time
import weakref
from datetime import datetime
from time import sleep

from sqlalchemy import create_engine, text
from sqlalchemy import event, exc

from airflow.utils.session import create_session


def plog(msg):
    now = datetime.now().isoformat()
    print(f"[{now}] in [os:{os.getpid()}] {msg}")


def child_process(label):
    """자식 프로세스에서 실행"""
    plog(f"[{label}] PID={os.getpid()} started")
    try:
        sleep(10)

    except Exception as e:
        plog(f"[{label}] ERROR: {e}")

if __name__ == "__main__":

    global engine

    # 테스트용 엔진 (본인 환경에 맞게 바꾸세요)
    engine = create_engine("mysql://root:mysqlrootpass@172.23.237.101/airflow",
                           pool_size=5, max_overflow=10, pool_recycle=1800,         future= True,
                           echo_pool='debug'
    )


    @event.listens_for(engine, "connect")
    def set_mysql_timezone(dbapi_connection, connection_record):
        logging.debug("mysql connectio connect os_id: " + str(os.getpid()))
        logging.debug("connect connection_record")
        print(connection_record.info)

        cursor = dbapi_connection.cursor()
        cursor.execute("SET time_zone = '+00:00'")
        cursor.close()

        # connection_ids[connection_record] = os.getpid()
        logging.debug(f"[connect] New DB connection established, id={os.getpid()}")
        plog(dbapi_connection)
        plog(connection_record)

        # wr = weakref.ref(dbapi_connection)
        weakref.finalize(dbapi_connection,
                         weakref.finalize(dbapi_connection,
                                          lambda: print(
                                              f"{datetime.now().isoformat()} dbapi_connection finalized via weakref in os {os.getpid()}",
                                              )))
        weakref.finalize(connection_record, lambda: plog(
            f"{datetime.now().isoformat()} connection_record finalized via weakref in os {os.getpid()}"))


    @event.listens_for(engine, "checkout")
    def set_checkout(dbapi_connection, connection_record, a):
        plog("checkout")
        plog(dbapi_connection)
        plog(connection_record)
        plog(connection_record.dbapi_connection)


    def clean_in_fork():
        _globals = globals()

        if engine := _globals.get("engine"):
            plog(f"engine before dispose: {engine.pool.status()}")
            engine.dispose(close=False)
            plog(f"engine after dispose: {engine.pool.status()}")

    os.register_at_fork(after_in_child=clean_in_fork)

    # 부모에서 미리 connection 하나 열어둠
    plog(os.getpid())
    with engine.connect() as conn:
        cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
        plog(f"[parent] pre-opened connection id={cid}")

    # 자식 프로세스 2개 생성
    for i in range(2):
        pid = os.fork()
        if pid == 0:
            # child
            child_process(f"child-{i}")
            os._exit(0)

    # 부모는 자식 종료 대기
    for _ in range(2):
        sleep(3)
        with engine.connect() as conn:
            cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
            plog(f"[parent] pre-opened connection id={cid}")
        os.wait()
    print("[parent] all children finished")
