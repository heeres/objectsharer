# ipython --pylab=qt
# execfile('pythonclient.py')

import time
import numpy as np
import objectsharer as objsh
objsh.helper.backend.start_server('127.0.0.1')
objsh.helper.backend.connect_to('tcp://127.0.0.1:54322')

def benchmark(func, msg, N):
    start = time.time()
    for i in range(N):
        ret = func(msg)
    end = time.time()
    dt = (end - start) / N
    print '  msg size: %d' % len(msg)
    print '    %d calls, %.03f usec / call' % (N, dt * 1e6,)
    print '    Throughput: %.03f MB / sec' % (len(msg) / dt / 1e6, )

o = objsh.find_object('order_server')
o.arrays(np.ones(10))
o.arrays(np.ones(10), a2=2*np.ones(20))
o.arrays(np.ones(10), a3=3*np.ones(30))
o.arrays(np.ones(10), a2=2*np.ones(20), a3=3*np.ones(30))

objsh.helper.backend.add_qt_timer()

