"""Copyright 2011 Urban Airship"""
import errno
import os
import signal
import socket
import sys
import time
import traceback


RETRY_ERRORS = frozenset((
    None, # socket.timeout().errno is None
    errno.ENOENT,
    errno.EPIPE,
    errno.ECONNREFUSED,
    errno.ECONNRESET,
))


def connect(url):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    s.settimeout(0.2)
    s.connect(url)
    return s


def punish(me, num, url):
    s = connect(url)
    for i in range(num):
        logline = '%s %8s %s' % (str(me)*22, i, 'a'*64)
        reconnect = False
        while 1:
            if reconnect:
                # Throttle
                time.sleep(0.1)
                try:
                    s = connect(url)
                    reconnect = False
                except (socket.error, socket.timeout) as e:
                    print '{%s:%s exc connecting (%s)}' % (me, i, e)
                    s.close()
                    if e.errno in RETRY_ERRORS:
                        continue # Reconnect again
                    else:
                        raise # Unable to continue

            try:
                s.send(logline)
                break
            except (socket.error, socket.timeout) as e:
                print '[%s:%s exc sending (%s)]' % (me, i, e)
                s.close()
                if e.errno in RETRY_ERRORS:
                    reconnect = True
                else:
                    raise


def spawn(con, num, url, logd_pid):
    for i in range(con):
        pid = os.fork()
        if pid:
            print 'CHILD %d Started (%d)' % (i, pid)
            continue
        else:
            try:
                punish(i, num, url)
            except:
                traceback.print_exc()
                raise
            return True

    
    if logd_pid:
        for i in range(20):
            os.kill(logd_pid, signal.SIGHUP)
            print 'SIGHUP Sent'
            time.sleep(0.1)

def main():
    if len(sys.argv) == 5:
        _, concurrency, messages, url, logd_pid = sys.argv
    elif len(sys.argv) == 4:
        _, concurrency, messages, url = sys.argv
        logd_pid = 0
    else:
        print 'usage: %s <concurrency> <messages> <url> [logd pid]' % (
                sys.argv[0],)
        sys.exit(1)
    concurrency = int(concurrency)
    messages = int(messages)
    logd_pid = int(logd_pid)

    s = time.time()
    child = spawn(concurrency, messages, url, logd_pid)
    if child:
        return
    print 'SIGHUP Loop done, waiting on punishers'
    while 1:
        try:
            os.wait()
        except OSError:
            print 'CHILD processes already done'
            break
    e = time.time()
    total = concurrency * messages
    print 'FIN - Sent %d messages in %d seconds (%f per second)' % (
            total, e - s, total / (e - s))


if __name__ == '__main__':
    main()
