import logging
logging.getLogger().setLevel(logging.DEBUG)
import objectsharer as objsh

objsh.helper.backend.start_server('127.0.0.1', port=54322)

class Order(object):
    def __init__(self):
        pass

    def arrays(self, a1, a2=None, a3=None):
        if a1 is not None and a1[0] != 1:
            raise ValueError('a1 not 1!')
        if a2 is not None and a2[0] != 2:
            raise ValueError('a2 not 2!')
        if a3 is not None and a3[0] != 3:
            raise ValueError('a3 not 3!')
        print 'Arrays ok: %s, %s, %s' % (a1, a2, a3)

e = Order()
objsh.register(e, name='order_server')
objsh.helper.backend.main_loop()

