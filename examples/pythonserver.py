import logging
logging.getLogger().setLevel(logging.DEBUG)

import objectsharer as objsh
from objectsharer.objects import PythonInterpreter

objsh.helper.backend.start_server('127.0.0.1', port=54321)

py = PythonInterpreter()
objsh.register(py, name='python_server')

#objsh.helper.backend.add_qt_timer(20)
objsh.helper.backend.main_loop()


