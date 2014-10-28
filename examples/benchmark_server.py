import logging
logging.getLogger().setLevel(logging.DEBUG)
import cProfile

import objectsharer as objsh
from objectsharer.objects import EchoServer

objsh.helper.backend.start_server('127.0.0.1', port=54322)

e = EchoServer()
objsh.register(e, name='echo_server')

cProfile.run('objsh.helper.backend.main_loop()', sort='time')
#objsh.helper.backend.main_loop()
#objsh.helper.backend.add_qt_timer(2)


