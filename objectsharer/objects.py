# Some potentially useful shareable objects

class PythonInterpreter(object):

    def __init__(self, namespace={}):
        self._namespace = namespace

    def cmd(self, cmd):
        retval = eval(cmd, self._namespace, self._namespace)
        return retval

    def ip_queue(self, cmd):
        import code, threading, IPython
        c = code.compile_command(cmd)
        cev = threading.Event()
        rev = threading.Event()
        try:
            ip = IPython.core.ipapi.get()
        except:
            ip = IPython.ipapi.get()
        ip.IP.code_queue.put((c, cev, rev))

