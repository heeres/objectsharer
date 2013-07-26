# ipython --pylab=qt
# execfile('pythonclient.py')

import logging
logging.getLogger().setLevel(logging.DEBUG)

import objectsharer as objsh
zbe = objsh.ZMQBackend()
zbe.start_server('127.0.0.1')
zbe.connect_to('tcp://127.0.0.1:54321')

py = objsh.find_object('python_server')

zbe.add_qt_timer()


