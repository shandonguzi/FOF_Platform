
import time
from functools import wraps

def insure_successful_run(retry = 3):
    '''retry decorator'''
    def decorator(func):
        @wraps(func)
        def robust_func(*args, **kwargs):
            REDO = True
            trys = retry
            RETRY = retry
            while REDO and RETRY > 0:
                try:
                    result = func(*args,**kwargs)
                    REDO = False
                    return result
                except Exception as error:
                    RETRY -= 1
                    print(error)
                    print(f"[×] {time.strftime('%c')} failed")
                    time.sleep(5)
                    pass
            if RETRY == 0:
                print(f"[×] {time.strftime('%c')} all {trys} trys failed")
        return robust_func
    return decorator
    