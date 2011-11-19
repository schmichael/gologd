"""Copyright 2011 Urban Airship"""
import atexit
import ConfigParser
import datetime
import errno
import os
import select
import signal
import socket
import sys
import time
import traceback

try:
    import mmstats
except ImportError:
    print 'You need to install mmstats with either:'
    print
    print '    easy_install mmstats'
    print
    print '...or...'
    print
    print '    pip install mmstats'
    print
    print 'See https://github.com/schmichael/mmstats'
    sys.exit(1)

MAXSZ = 2048
CFG_SECTION = 'simplelogd'
LOG_DEBUG = 3
LOG_INFO = 2
LOG_NOTICE = 1


class LoggerdStats(mmstats.MmStats):
    messages = mmstats.CounterField()
    retries = mmstats.CounterField()
    start = mmstats.StaticInt64Field(value=lambda: int(time.time()))
    clients = mmstats.CounterField()
    reloads = mmstats.CounterField()
    syncs = mmstats.CounterField()


class Loggerd(object):
    def __init__(self, config_filename):
        self.stats = LoggerdStats(label_prefix='simplelogd.')
        atexit.register(self.stats.remove)
        self.config_filename = config_filename

        self.clients = {}
        self.inputs = []
        self._run = True
        self.files_to_cleanup = set()
        self.log_filename = None
        self.log_file = None
        self.url = None
        self.sock = None
        self.clean_shutdown_time = None
        self.log_level = LOG_NOTICE

        self.load_config()
        self.listen()

        signal.signal(signal.SIGHUP, self.handle_hup)
        signal.signal(signal.SIGTERM, self.handle_term)
        signal.signal(signal.SIGQUIT, self.handle_term)

        self.notice(
                "Started simplelogd on %s (%d)" % (self.url, os.getpid()))

    def listen(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        if os.path.exists(self.url):
            os.remove(self.url)
        self.sock.bind(self.url)
        self.files_to_cleanup.add(self.url)
        # Hardcode listen backlog fairly low; we shouldn't let this back
        # up too far or we're just blocking clients
        self.sock.listen(10)
        self.inputs = [self.sock]

    def cleanup_files(self):
        for f in self.files_to_cleanup:
            try:
                os.remove(f)
            except OSError:
                self.notice(traceback.format_exc())

    def debug(self, msg):
        if self.log_level >= LOG_DEBUG:
            print "[%s] [debug] %s" % (datetime.datetime.utcnow(), msg)

    def info(self, msg):
        if self.log_level >= LOG_INFO:
            print "[%s] [info] %s" % (datetime.datetime.utcnow(), msg)

    def notice(self, msg):
        if self.log_level >= LOG_NOTICE:
            print "[%s] [notice] %s" % (datetime.datetime.utcnow(), msg)

    def load_config(self):
        """Loads config from ini file; safe to re-run"""
        self.info("Reading config file: %s" % self.config_filename)
        with open(self.config_filename) as f:
            conf = ConfigParser.ConfigParser()
            conf.readfp(f)

        self.log_level = conf.getint(CFG_SECTION, 'log_level')

        # PID files seem like a good idea
        self.pid_file = conf.get(CFG_SECTION, 'pid_file')
        if self.pid_file:
            with open(self.pid_file, 'wb') as f:
                f.write("%s\n" % os.getpid())
            self.files_to_cleanup.add(self.pid_file)

        # Read log filename
        self.log_filename = conf.get(CFG_SECTION, 'log_file')
        if self.log_file:
            # Close the currently open log file before opening a new one
            self.log_file.close()

        # Open log file for appending
        self.log_file = open(self.log_filename, 'a+b')
        self.log_filefd = self.log_file.fileno()

        # Read log file sync rate (sync per N messages)
        self.sync_rate = conf.getint(CFG_SECTION, 'sync_rate')

        # Load URL but only if it's not already set - changing at runtime is
        # not allowed
        if self.url is None:
            self.url = conf.get(CFG_SECTION, 'url')
        else:
            if self.url != conf.get(CFG_SECTION, 'url'):
                self.notice(
                    'Url changed, restart to apply. Current: %s - Config: %s'
                    % (self.url, conf.get(CFG_SECTION, 'url'))
                )

        self.clean_shutdown_time = conf.getfloat(CFG_SECTION,
                'clean_shutdown_time')

    def handle_hup(self, signum, frame):
        self.info('%d messages processed' % self.stats.messages.value)
        self.notice('HUP received, reload config')
        self.load_config()
        self.stats.reloads.inc()

    def handle_term(self, signum, frame):
        if signum == signal.SIGQUIT:
            self.notice('QUIT received, stopping')
        else:
            self.notice('TERM received, stopping')
        self._run = False
        self.sync_log()

    def _accept_client(self):
        new_conn, addr = self.sock.accept()
        self.inputs.append(new_conn)
        self.clients[new_conn] = addr
        self.debug("Client connected: %d" % new_conn.fileno())
        self.stats.clients.inc(1)

    def _close_client(self, client_sock):
        self.debug("Client disconnected: %d" % client_sock.fileno())
        self.inputs.remove(client_sock)
        client_sock.close()
        self.stats.clients.inc(-1)

    def run(self):
        """Main event loop"""
        self.info('Waiting for messages')
        i = self.stats.messages
        client_count = self.stats.clients
        outputs, excs = [], [] # Unused, loggerd only reads
        while self._run:
            try:
                r, _, _ = select.select(self.inputs, outputs, excs)
            except KeyboardInterrupt:
                self.debug("Ctrl-C - Stopping")
                self._run = False
                break
            except select.error as e:
                errcode, _ = e
                if errcode == errno.EINTR:
                    self.debug("Interrupt during select")
                    continue
                else:
                    # Other possibilities are unrecoverable:
                    # EBADF - not sure how to hit this except a programming error
                    # EINVAL - programming error
                    # ENOMEM - lol we're screwed, bail
                    raise

            for s in r:
                if s is self.sock:
                    self._accept_client()
                else:
                    try:
                        d = s.recv(MAXSZ)
                    except socket.error as e:
                        if e.errno == errno.EINTR:
                            # Interrupt
                            self.debug("Interrupt during socket.recv()")
                        else:
                            # Kill misbehaving clients and let them reconnect
                            self._close_client(s)
                        continue

                    if not d:
                        # Client disconnected
                        self._close_client(s)
                    else:
                        try:
                            self.log_file.write(d+"\n")
                        except ValueError:
                            # Attemptted to write to a closed log file, retry
                            self.stats.retries.inc()
                            self.log_file.write(d+"\n")
                        i.inc()

                        if self.sync_rate and i.value % self.sync_rate == 0:
                            self.sync_log()

    def cleanup_sockets(self):
        """Shuts down sockets cleanly so as to not lose messages"""
        self.info("Cleaning up sockets")
        # Disable HUP & TERM handling
        signal.signal(signal.SIGHUP, lambda s, f: None)
        signal.signal(signal.SIGTERM, lambda s, f: None)
        signal.signal(signal.SIGQUIT, lambda s, f: None)

        # Don't accept anymore connections
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.inputs.remove(self.sock)

        # First shutdown sockets
        for s in self.inputs:
            s.shutdown(socket.SHUT_RDWR)

        # Sleep appears to workaround a race condition where shutdown sockets
        # may appear exhausted before they really are
        time.sleep(self.clean_shutdown_time)

        # Then read out pending messages
        messages_saved = 0
        for s in self.inputs:
            while 1:
                try:
                    d = s.recv(MAXSZ)
                except socket.error:
                    # This socket has failed us, bail
                    break

                if d:
                    self.log_file.write(d+"\n")
                    messages_saved += 1
                    self.stats.messages.inc()
                else:
                    break
            self.debug("Disconnecting client: %s" % s.fileno())
            s.close()
            self.stats.clients.inc(-1)
        self.debug("Pending messages saved: %d" % messages_saved)

    def sync_log(self):
        self.log_file.flush()
        os.fsync(self.log_filefd)
        self.stats.syncs.inc()


def main():
    fn = sys.argv[1]
    logger = Loggerd(fn)
    try:
        logger.run()
    finally:
        logger.cleanup_sockets()
        logger.sync_log()
        logger.cleanup_files()


if __name__ == '__main__':
    main()
