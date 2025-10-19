#!/usr/bin/env python3
import os
import time
import weakref
import gc


class SimpleObject:
    def __init__(self, name):
        self.name = name

    def __del__(self):
        print(f"[PID:{os.getpid()}] {self.name} 객체가 GC됨!")


def test_fork_gc():
    print("=== Fork GC 테스트 시작 ===")

    # 1. 부모에서 global 객체 생성
    global test_obj
    test_obj = SimpleObject("부모_객체")

    # 2. weakref로 객체 추적
    weak_ref = weakref.ref(test_obj)
    finalizer = weakref.finalize(test_obj,
                                 lambda: print(f"[PID:{os.getpid()}] finalize: 부모_객체 정리됨!"))

    print(f"[부모 PID:{os.getpid()}] 부모 객체 생성 - ID: {id(test_obj)}")
    print(f"[부모] 객체 존재 여부: {weak_ref() is not None}")

    def clean_in_fork():
        """Fork 후 자식에서 실행될 함수"""
        global test_obj

        print(f"[자식 PID:{os.getpid()}] Fork 후 초기 객체 ID: {id(test_obj)}")
        print(f"[자식] 객체 내용: {test_obj.name}")

        # 자식에서 global 변수 재할당
        old_obj = test_obj  # 기존 객체 참조 보관
        test_obj = SimpleObject("자식_객체")  # 새로운 객체로 재할당

        print(f"[자식] 재할당 후 새 객체 ID: {id(test_obj)}")
        print(f"[자식] 새 객체 내용: {test_obj.name}")

        # 기존 객체 참조 해제 (자식에서만 GC됨을 확인)
        del old_obj
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
        print(f"[자식] 최종 객체: {test_obj.name}")
        print("[자식] 프로세스 종료")
        os._exit(0)

    else:  # 부모 프로세스
        print(f"[부모 PID:{os.getpid()}] 자식 프로세스 대기중...")

        # 부모에서 객체가 여전히 존재하는지 확인
        time.sleep(0.5)
        print(f"[부모] Fork 후 객체 여전히 존재: {test_obj.name}")
        print(f"[부모] 객체 ID: {id(test_obj)}")
        print(f"[부모] weakref 객체 존재: {weak_ref() is not None}")

        # 자식 프로세스 종료 대기
        os.waitpid(pid, 0)
        print("[부모] 자식 프로세스 종료됨")

        # 부모에서 여전히 원래 객체가 살아있는지 최종 확인
        print(f"[부모] 최종 확인 - 객체: {test_obj.name}")
        print(f"[부모] 최종 확인 - weakref 존재: {weak_ref() is not None}")

        print("\n=== 부모에서 객체 수동 삭제 테스트 ===")
        test_obj = None  # 부모에서 명시적 삭제
        gc.collect()
        print(f"[부모] 수동 삭제 후 weakref 존재: {weak_ref() is not None}")


if __name__ == "__main__":
    test_fork_gc()
    print("\n=== 테스트 완료 ===")
