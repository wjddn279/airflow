import logging
import os
import time
import weakref
from datetime import datetime
from time import sleep
import gc

from sqlalchemy import create_engine, text, event

def plog(msg):
    now = datetime.now().isoformat()
    print(f"[{now}] in [os:{os.getpid()}] {msg}", flush=True)

def child_process(label):
    plog(f"[{label}] PID={os.getpid()} started")
    try:
        sleep(10)
    except Exception as e:
        plog(f"[{label}] ERROR: {e}")

if __name__ == "__main__":
    global engine

    engine = create_engine(
        "mysql+mysqldb://root:mysqlrootpass@172.23.237.101/airflow",
        pool_size=5, max_overflow=10, pool_recycle=1800, future=True,
        echo_pool='debug'
    )

    @event.listens_for(engine, "connect")
    def set_mysql_timezone(dbapi_connection, connection_record):
        logging.debug("mysql connectio connect os_id: " + str(os.getpid()))
        logging.debug("connect connection_record")
        print(connection_record.info)
        cur = dbapi_connection.cursor()
        cur.execute("SET time_zone = '+00:00'")
        cur.close()
        logging.debug(f"[connect] New DB connection established, id={os.getpid()}")
        plog(dbapi_connection)
        plog(connection_record)

        # 객체 자체를 캡처하지 말고 식별자만 캡처
        conn_id = id(dbapi_connection)
        rec_id = id(connection_record)
        weakref.finalize(
            dbapi_connection,
            lambda cid=conn_id: print(f"{datetime.now().isoformat()} dbapi_connection finalized via weakref in os {os.getpid()} id={cid}", flush=True)
        )
        weakref.finalize(
            connection_record,
            lambda rid=rec_id: plog(f"{datetime.now().isoformat()} connection_record finalized via weakref in os {os.getpid()} id={rid}")
        )

    @event.listens_for(engine, "checkout")
    def set_checkout(dbapi_connection, connection_record, a):
        plog("checkout")
        plog(dbapi_connection)
        plog(connection_record)
        plog(connection_record.dbapi_connection)

    # 자식에서 엔진 레퍼런스를 끊고 GC로 수거(명시 close 호출 없음)
    def clean_in_fork():
        _globals = globals()
        if engine := _globals.get("engine"):
            print("engine before dispose: %s", engine.pool.status())
            engine.dispose(close=False)
            print("engine after dispose: %s", engine.pool.status())

    os.register_at_fork(after_in_child=clean_in_fork)

    # 부모에서 미리 connection 하나 열어둠(풀에 최소 1개 생성)
    plog(os.getpid())
    with engine.connect() as conn:
        cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
        plog(f"[parent] pre-opened connection id={cid}")

    # 자식 프로세스 2개 생성
    for i in range(2):
        pid = os.fork()
        if pid == 0:
            child_process(f"child-{i}")
            os._exit(0)

    # 부모: 주기적으로 쿼리 실행하여 자식 finalize 직후 2013 관찰
    for _ in range(10):
        try:
            with engine.connect() as conn:
                cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
                plog(f"[parent] query ok on connection id={cid}")
        except Exception as e:
            plog(f"[parent] expected failure: {type(e).__name__}: {e}")
        sleep(1)

    print("[parent] all children finished")
