import logging
logging.getLogger().setLevel(logging.DEBUG)

import objectsharer as objsh
from objectsharer.objects import PythonInterpreter

zbe = objsh.ZMQBackend()
zbe.start_server('127.0.0.1', port=54321)

py = PythonInterpreter()
objsh.register(py, name='python_server')

zbe.main_loop()


