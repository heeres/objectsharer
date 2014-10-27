# ipython --pylab=qt -i pythonclient.py

import logging
logging.getLogger().setLevel(logging.DEBUG)

import objectsharer as objsh
objsh.helper.backend.start_server('127.0.0.1')
objsh.helper.backend.connect_to('tcp://127.0.0.1:54321')

print 'Finding object...'
py = objsh.find_object('python_server')
print 'Reply: %s' % py.cmd('1+1')

objsh.helper.backend.add_qt_timer(20)

