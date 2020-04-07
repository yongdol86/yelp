import time
from functools import wraps


def timer(func):
    '''
    decorator function to automatically add timer
    '''

    @wraps(func)
    def _wrapper(*args, **kwargs):
        timer = Timer()
        timer.start(func.__name__)

        response = func(*args, **kwargs)

        timer.stop()

        return response

    return _wrapper


class Timer:
    def __init__(self):
        self.label = None
        self._start_time = None

    def start(self, label=None):
        if self._start_time is not None:
            raise RuntimeError

        self.label = label
        self._start_time = time.perf_counter()

        if self.label:
            print(f'{self.label} started')

    # TODO: debug 플래그 켜졌을 때만 출력하도록 바꾸기?
    def stop(self):
        if self._start_time is None:
            raise RuntimeError

        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        message = f'elapsed time: {elapsed_time:0.4f} sec \n'

        if self.label:
            message = f'{self.label} finished, ' + message
            self.label = None

        print(message)
