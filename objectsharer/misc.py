class RemoteException(Exception):
    pass

class TimeoutError(RuntimeError):
    pass

def ellipsize(s):
    if len(s) > 64:
        return s[:64] + '...'
    else:
        return s

