#!/usr/bin/env python3

import functools
import os
import time
import weakref
import gc
from datetime import datetime

from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import scoped_session, sessionmaker


def test_fork_gc():
    print("=== SQLAlchemy Engine Fork GC 테스트 시작 ===")

    # 1. 부모에서 global engine 생성
    global engine, NonScopedSession, Session
    engine = create_engine(
        "mysql://root:mysqlrootpass@172.23.237.101/airflow",
        pool_size=3,
        max_overflow=2,
        pool_recycle=1800,
        # echo_pool='debug',  # pool 디버그 메시지 표시
        pool_pre_ping=True,
        future=True
    )

    # 2. weakref로 engine 추적
    weak_ref = weakref.ref(engine)
    finalizer = weakref.finalize(engine,
                                 lambda: print(f"[PID:{os.getpid()}] finalize: 부모_engine 정리됨!"))

    @event.listens_for(engine, "connect")
    def set_mysql_timezone(dbapi_connection, connection_record):
        # connection_ids[connection_record] = os.getpid()
        print(f"[connect] New DB connection established, id={os.getpid()}")
        print(dbapi_connection)
        print(connection_record)

        weakref.finalize(dbapi_connection,
                         lambda: print(
                             f"{datetime.now().isoformat()} dbapi_connection finalized via weakref in os {os.getpid()}",
                             ))
        weakref.finalize(connection_record, lambda: print(
            f"{datetime.now().isoformat()} connection_record finalized via weakref in os {os.getpid()}"))

    _session_maker = functools.partial(
        sessionmaker,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )
    NonScopedSession = _session_maker(engine)
    Session = scoped_session(NonScopedSession)

    print(f"[부모 PID:{os.getpid()}] 부모 engine 생성 - ID: {id(engine)}")
    print(f"[부모] engine URL: {engine.url}")
    print(f"[부모] weakref 객체 존재 여부: {weak_ref() is not None}")

    with engine.connect() as conn:
        cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
        print(f"[parent] pre-opened connection id={cid}")

    def clean_in_fork():
        """Fork 후 자식에서 실행될 함수"""
        global engine, NonScopedSession, Session

        print(f"[자식 PID:{os.getpid()}] Fork 후 초기 engine ID: {id(engine)}")
        print(f"[자식] 기존 engine URL: {engine.url}")

        # 자식에서 기존 engine 정리 (SQLAlchemy 권장사항)
        # try:
        #     engine.dispose(close=False)  # 연결 풀 정리
        #     print(f"[자식] 기존 engine.dispose() 완료")
        # except Exception as e:
        #     print(f"[자식] dispose 중 에러: {e}")

        # 자식에서 새로운 engine 생성
        engine = create_engine(
            "mysql://root:mysqlrootpass@172.23.237.101",
            pool_size=3,
            max_overflow=2,
            pool_recycle=1800,
            # echo_pool='debug',  # pool 디버그 메시지 표시
            pool_pre_ping=True,
            future=True
        )

        weakref.finalize(engine,
                                     lambda: print(f"[PID:{os.getpid()}] finalize: 자식_engine 정리됨!"))

        NonScopedSession = _session_maker(engine)
        Session = scoped_session(NonScopedSession)

        print(f"[자식] 재할당 후 새 engine ID: {id(engine)}")
        print(f"[자식] 새 engine URL: {engine.url}")

        # 기존 engine 참조 해제 (자식에서만 GC됨을 확인)
        gc.collect()  # 강제 GC

    # 3. Fork 핸들러 등록
    if hasattr(os, 'register_at_fork'):
        os.register_at_fork(after_in_child=clean_in_fork)
        print("[부모] Fork 핸들러 등록됨")
    else:
        print("register_at_fork를 지원하지 않는 플랫폼입니다")
        return

    # 4. Fork 실행
    print("\n=== Fork 실행 ===")
    pid = os.fork()

    if pid == 0:  # 자식 프로세스
        print(f"[자식 PID:{os.getpid()}] 자식 프로세스 시작")
        time.sleep(1)  # 충분한 시간 주기
        print(f"[자식] 최종 engine ID: {id(engine)}")
        print(f"[자식] 최종 engine URL: {engine.url}")
        print("[자식] 프로세스 종료")

        os._exit(0)

    else:  # 부모 프로세스
        print(f"[부모 PID:{os.getpid()}] 자식 프로세스 대기중...")

        # 부모에서 engine이 여전히 존재하는지 확인
        time.sleep(0.5)
        print(f"[부모] Fork 후 engine 여전히 존재 - ID: {id(engine)}")
        print(f"[부모] engine URL: {engine.url}")
        print(f"[부모] weakref 객체 존재: {weak_ref() is not None}")

        # 자식 프로세스 종료 대기
        os.waitpid(pid, 0)
        print("[부모] 자식 프로세스 종료됨")

        # 부모에서 여전히 원래 engine이 살아있는지 최종 확인
        print(f"[부모] 최종 확인 - engine ID: {id(engine)}")
        print(f"[부모] 최종 확인 - engine URL: {engine.url}")
        print(f"[부모] 최종 확인 - weakref 존재: {weak_ref() is not None}")

        print("\n=== 부모에서 쿼리 한번 더 테스트 ===")

        with engine.connect() as conn:
            cid = conn.execute(text("SELECT CONNECTION_ID()")).scalar()
            print(f"[parent] pre-opened connection id={cid}")
        # try:
        #     engine.dispose()  # 연결 정리
        #     print("[부모] engine.dispose() 완료")
        # except Exception as e:
        #     print(f"[부모] dispose 중 에러: {e}")

        # engine = None  # 부모에서 명시적 삭제
        # gc.collect()
        print(f"[부모] 수동 삭제 후 weakref 존재: {weak_ref() is not None}")


if __name__ == "__main__":
    test_fork_gc()
    print("\n=== 테스트 완료 ===")
