import socket
import logging
import time
import select
import backend
import struct

BUFSIZE = 65536

logger = backend.get_logger('socket backend')

class SocketBackend(backend.Backend):

    def __init__(self, helper):
        super(SocketBackend, self).__init__(helper)
        self._srv_sock = None
        self._select_socks = list()
        self._rcv_bufs = dict()
        self._send_queue = dict()

    def do_connect(self, addr):
        '''
        Connect to a remote ObjectSharer at <addr>.
        If <uid> is specified it is associated with the client at <addr>.
        If <async> is False (default), wait for a reply.
        '''

        logger.debug('Connecting to %s', addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(backend.parse_addr(addr))
        logging.debug('Adding socket %s to select_socks', sock)
        self._select_socks.append(sock)
        return sock

    def poll(self, timeout):
        '''
        Poll all available sockets and accept connections if new ones
        available.
        '''

        try:
            rsocks, wlist, xlist = select.select(self._select_socks, [], [], timeout/1000.0)
        except Exception, e:
            logging.warning('select failed: %s, timeout %s, checking sockets', str(e), timeout)
            for s in self._select_socks:
                print 'Sock: %s (%d)' % (s, s.fileno())
            return []

        # Handle incoming connections
        if self._srv_sock in rsocks:
            clt_sock, clt_addr = self._srv_sock.accept()
            logging.debug('Accepted new socket, adding %s to select_socks', clt_sock.fileno())
            self._select_socks.append(clt_sock)

            # Remove srv_sock from the list to read from
            for i, sock in enumerate(rsocks):
                if sock == self._srv_sock:
                    del rsocks[i]

        return rsocks

    def send_data_to(self, sock, data):
        if sock not in self._send_queue:
            self._send_queue[sock] = []
        self._send_queue[sock].append(data)
        self.flush_send_queue()

    def _sock_send(self, sock, data):
        try:
            ret = sock.send(data)
        except socket.error, e:
            if e.errno not in (10035, ):
                logging.warning('Send exception (%s), assuming client disconnected', e)
                self.client_disconnected(sock)
                return -1
            ret = 0

        return ret

    def client_disconnected(self, sock):
        for i, isock in enumerate(self._select_socks):
            if isock == sock:
                del self._select_socks[i]
                break
        super(SocketBackend, self).client_disconnected(sock)

    def flush_send_queue(self, sock=None):
        '''
        Flush the per-connection send queue.
        (or just for socket <sock> if requested)
        '''

        if sock:
            socklist = [sock]
        else:
            socklist = self._send_queue.keys()

        for sock in socklist:
            datalist = self._send_queue[sock]
            while len(datalist) > 0:
                nsent = self._sock_send(sock, datalist[0])

                # Ok
                if nsent == len(datalist[0]):
                    del datalist[0]

                # Failed, signals dissockection so remove send queue
                elif nsent == -1:
                    del self._send_queue[sock]
                    logging.info('Sending failed, assuming dissockect')
                    self.client_dissockected(sock)

                # Partially sent, remove that part from queue
                else:
                    datalist[0] = datalist[0][nsent:]
                    logging.info('Sent paritally, %d bytes remaining', len(datalist[0]))
                    break

        return True

    def start_server(self, addr='', port=None):
        '''
        Start accepting connections on IP address <addr> and <port>.
        '''

        if addr == '*':
            addr = ''
        self.addr = addr
        self.port = port

        self._srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if port is None:
            self._srv_sock.bind((self.addr, 0))
            self.port = self._srv_sock.getsockname()[1]
        else:
            self._srv_sock.bind((self.addr, self.port))

        self._srv_sock.listen(5)
        self._select_socks.append(self._srv_sock)

        logger.debug('ObjectSharer listening at %s', self.get_server_address())

    def get_server_address(self):
        return 'tcp://%s:%s' % (self.addr, self.port)

    def _check_rcv_buf(self, sock):
        # Grab complete packet if available
        b = self._rcv_bufs[sock]
        ret = []
        while len(b) > 7:
            # Check packet magic
            if b[0:2] != 'OS':
                del self._rcv_bufs[sock]
                logging.warning('Packet magic missing, dropping data')
                return ret

            # Get data length
#            datalen = (ord(b[2]) << 24) + (ord(b[3]) << 16) + (ord(b[4]) << 8)  + ord(b[5])
            datalen = struct.unpack('<I', b[2:6])[0]
            if len(b) < datalen:
#                logging.debug('Incomplete packet received (expecting %s, got %s)', datalen, len(b))
                return ret

            # Get message parts
            nparts = ord(b[6])
            parts = []
            ofs = 7
            for i in range(nparts):
#                partlen = (ord(b[ofs]) << 24) + (ord(b[ofs+1]) << 16) + (ord(b[ofs+2]) << 8)  + ord(b[ofs+3])
                partlen = struct.unpack('<I', b[ofs:ofs+4])[0]
                if ofs + 4 + partlen > len(b):
                    logging.warning('Packet size problem, dropping data')
                    del self._rcv_bufs[sock]
                    return ret
                parts.append(b[ofs+4:ofs+4+partlen])
                ofs += 4 + partlen

            # Remove packet from buffer
            b = b[datalen:]
            self._rcv_bufs[sock] = b
            msg = backend.Message(sock, self.sock_to_uid_map.get(sock, None), parts)
            ret.append(msg)
        return ret

    def recv_from(self, sock):
        try:
            data = sock.recv(BUFSIZE)
        except Exception, e:
            logging.warning('Recv exception (%s), assuming client disconnected', e)
            self.client_disconnected(sock)
            return None
            
        if data == '':
            logging.warning('Empty recv on blocking call, assuming client disconnected')
            self.client_disconnected(sock)
            return None
        if sock not in self._rcv_bufs:
            self._rcv_bufs[sock] = data
        else:
            self._rcv_bufs[sock] += data

        return self._check_rcv_buf(sock)
