import io
import logging
import os
import signal
import sys
import time
import tracemalloc
import weakref
import gc
from datetime import datetime
from pathlib import Path, PosixPath
from select import select
from socket import socketpair
from time import sleep
from sqlalchemy import create_engine, text, event

from airflow.dag_processing.manager import DagFileInfo
from airflow.models import DagBag
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.sdk.execution_time.supervisor import block_orm_access
from airflow.settings import Session
from airflow.utils.session import provide_session, NEW_SESSION

# 로깅 설정
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')

# 전역 변수들
connection_ids = {}  # 이것을 사용하면 문제가 해결됨
parent_pid = os.getpid()


def plog(msg):
    """프로세스별 로그 출력"""
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    pid = os.getpid()
    process_type = "PARENT" if pid == parent_pid else "CHILD"
    print(f"[{now}] [{process_type}:{pid}] {msg}", flush=True)


# def test_connection(engine, label):
#     """DB 연결 테스트"""
#     try:
#         with engine.connect() as conn:
#             cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
#             plog(f"{label} - MySQL Connection ID: {cid}")
#             return cid
#     except Exception as e:
#         plog(f"{label} - Connection FAILED: {e}")
#         return None


def child_process(child_id):
    """자식 프로세스에서 실행되는 함수"""

    bag = DagBag(
        dag_folder='/opt/airflow/dags/dags/jp/dynamic_dags_jp_vinitus.py',
        bundle_path=PosixPath('/opt/airflow/dags/dags'),
        include_examples=False,
        load_op_links=False,
    )

    plog(f"Child-{child_id} finished")


def _close_unused_sockets(*sockets):
    """Close unused ends of sockets after fork."""
    for sock in sockets:
        sock.close()

def _reset_signals():
    # Uninstall the rich etc. exception handler
    sys.excepthook = sys.__excepthook__
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGUSR2, signal.SIG_DFL)


def _reopen_std_io_handles(child_stdin, child_stdout, child_stderr):
    # Ensure that sys.stdout et al (and the underlying filehandles for C libraries etc) are connected to the
    # pipes from the supervisor
    for handle_name, fd, sock, mode in (
        # Yes, we want to re-open stdin in write mode! This is cause it is a bi-directional socket, so we can
        # read and write to it.
        ("stdin", 0, child_stdin, "w"),
        # ("stdout", 1, child_stdout, "w"),
        ("stderr", 2, child_stderr, "w"),
    ):
        os.dup2(sock.fileno(), fd)
        del sock

        # We open the socket/fd as binary, and then pass it to a TextIOWrapper so that it looks more like a
        # normal sys.stdout etc.
        binary = os.fdopen(fd, mode + "b")
        handle = io.TextIOWrapper(binary, line_buffering=True)
        setattr(sys, handle_name, handle)


def main(use_connection_ids=False, use_register_at_fork=True):
    """
    메인 함수

    Args:
        use_connection_ids: True면 connection_ids에 참조 저장 (문제 해결)
        use_register_at_fork: True면 fork 후 engine.dispose() 호출
    """

    print("\n" + "=" * 80)
    print(f"TEST CONFIGURATION:")
    print(f"  - use_connection_ids: {use_connection_ids}")
    print(f"  - use_register_at_fork: {use_register_at_fork}")
    print("=" * 80 + "\n")
    #
    global engine1
    #
    # # MySQL 연결 (본인 환경에 맞게 수정)
    engine1 = create_engine(
        "postgresql://admin:admin123@172.23.115.103/airflow",
        pool_size=3,
        max_overflow=2,
        pool_recycle=1800,
        echo_pool='debug',  # pool 디버그 메시지 표시
        pool_pre_ping=True,
        future=True
    )

    engine1 = create_engine(
        "mysql://root:mysqlrootpass@172.23.237.101/airflow",
        pool_size=3,
        max_overflow=2,
        pool_recycle=1800,
        echo_pool='debug',  # pool 디버그 메시지 표시
        pool_pre_ping=True,
        future=True
    )

    # 연결 이벤트 리스너
    @event.listens_for(engine1, "connect")
    def set_mysql_timezone(dbapi_connection, connection_record):
        pid = os.getpid()
        plog(f"==> CONNECT event fired")

        # 타임존 설정
        # cursor = dbapi_connection.cursor()
        # cursor.execute("SET time_zone = '+00:00'")
        # cursor.close()

        # weakref로 GC 시점 추적
        weakref.finalize(
            dbapi_connection,
            lambda: plog(f"!!! dbapi_connection GC'd (was created by PID {pid})")
        )
        weakref.finalize(
            connection_record,
            lambda: plog(f"!!! connection_record GC'd (was created by PID {pid})")
        )

    # 체크아웃 이벤트 리스너
    @event.listens_for(engine1, "checkout")
    def receive_checkout(dbapi_connection, connection_record, connection_proxy):
        plog(f"==> CHECKOUT event fired")

    # register_at_fork 설정
    if use_register_at_fork:
        def clean_in_fork():
            _globals = globals()
            if engine := _globals.get("engine"):
                plog(">>> Calling engine.dispose() in forked child")
                engine.dispose(close=False)

            print(engine)

        if hasattr(os, 'register_at_fork'):
            os.register_at_fork(after_in_child=clean_in_fork)
            plog("register_at_fork configured")

    def _get_priority_files():
        with engine1.connect() as conn:
            cid = conn.execute(text("SELECT * FROM job")).scalar()
            print(cid)

    # 부모 프로세스에서 connection pool 초기화
    plog("Parent initializing connection pool...")
    print(_get_priority_files())

    plog("\n" + "-" * 60)
    plog("Forking child processes...")
    plog("-" * 60 + "\n")

    # 자식 프로세스 생성
    children = []

    child_stdout, read_stdout = socketpair()
    child_stderr, read_stderr = socketpair()

    # Place for child to send requests/read responses, and the server side to read/respond
    child_requests, read_requests = socketpair()

    # Open the socketpair before forking off the child, so that it is open when we fork.
    child_logs, read_logs = socketpair()

    for i in range(2):
        pid = os.fork()
        if pid == 0:
            # 자식 프로세스

            # gc.disable()
            # gc.set_threshold(2000, 50, 50)
            # gc.set_debug(gc.DEBUG_COLLECTABLE)
            try:
                child_process(i)
                gc.collect()
            except Exception as e:
                plog(f"Child-{i} error: {e}")
            finally:
                os._exit(0)
        else:
            # 부모 프로세스
            children.append(pid)
            plog(f"Forked child PID: {pid}")
            sleep(0.5)

    # 부모는 계속 쿼리 실행
    plog("\nParent continuing to use connections...")
    for i in range(100):
        sleep(3)
        result = _get_priority_files()
        plog(result)

    # 자식 프로세스 종료 대기
    for pid in children:
        os.waitpid(pid, 0)
        plog(f"Child {pid} terminated")

    plog("\n" + "=" * 60)
    plog("TEST COMPLETED")
    plog("=" * 60)

    # 최종 상태 확인
    # print(f"\nFinal pool status: {engine.pool.status()}")
    if connection_ids:
        print(f"connection_ids has {len(connection_ids)} entries")


if __name__ == "__main__":
    # 테스트 1: 문제 재현 (connection이 끊어짐)
    print("\n\n### TEST 1: Problem Reproduction (connections will be lost) ###")
    main(use_connection_ids=False, use_register_at_fork=True)

    sleep(2)

    # # 테스트 2: connection_ids 사용하여 문제 해결
    # print("\n\n### TEST 2: With connection_ids (connections maintained) ###")
    # main(use_connection_ids=True, use_register_at_fork=True)
    #
    # sleep(2)
    #
    # # 테스트 3: register_at_fork 없이 테스트
    # print("\n\n### TEST 3: Without register_at_fork (connections maintained) ###")
    # main(use_connection_ids=False, use_register_at_fork=False)
