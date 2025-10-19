import gc
import weakref

class TestObject:
    def __del__(self):
        print("객체가 삭제되었습니다")

def print_a():
    print("object has gone")

order = TestObject()
weak_ref = weakref.finalize(order, print_a)
print("before")
order = "new value"  # 기존 객체 해제됨
print("after")
print(weak_ref() is None)  # True - 객체가 삭제됨
