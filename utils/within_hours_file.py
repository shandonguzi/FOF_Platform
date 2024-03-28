'''judge a file is created within hours or not'''
import time
import os

def is_within(file_path, hours):
    '''judge a file is created within hours or not'''
    if os.path.exists(file_path):
        return (time.time() - os.stat(file_path).st_ctime) < 3600 * hours
    else:
        return False
