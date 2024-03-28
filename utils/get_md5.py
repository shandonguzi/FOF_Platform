'''provides md5 value'''

import hashlib

def get_md5(path):
    '''get md5 of file'''
    hash_md5 = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda : f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()
