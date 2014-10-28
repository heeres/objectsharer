# ipython --pylab=qt
# execfile('pythonclient.py')

import time
import numpy as np
import objectsharer as objsh

zbe = objsh.ZMQBackend()
zbe.start_server('127.0.0.1')
zbe.connect_to('tcp://127.0.0.1:54321')

def benchmark(func, msg, N):
    start = time.time()
    for i in range(N):
        ret = func(msg)
    end = time.time()
    dt = (end - start) / N
    print '  msg size: %d' % len(msg)
    print '    %d calls, %.03f usec / call' % (N, dt * 1e6,)
    print '    Throughput: %.03f MB / sec' % (len(msg) / dt / 1e6, )

e = objsh.find_object('echo_server')
size_N = (
    (10, 5000),
    (100, 5000),
    (1000, 5000),
    (10000, 5000),
    (100000, 500),
    (1000000, 500),
)

print 'Using python strings:'
for size, N in size_N:
    ar = '0' * size
    benchmark(e.echo, ar, N)

print 'Using numpy arrays:'
for size, N in size_N:
    ar = np.random.random(size) * 255
    ar = ar.astype(np.uint8)
    benchmark(e.echo, ar, N)

objsh.helper.backend.add_qt_timer()

