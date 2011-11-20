gologd
======

A simple multiplexer: multiple writers send messages via a Unix Domain Socket
which gologd writes to a log file.

*Warning:* Seems to only work in Linux despite OSX's socket manpage mentioning
SOCK_SEQPACKET.

*Warning:* It's stupid slow right now (approx. 3x slower than a Python version
that does more) and meant to be a learning experience.

python_logd.py
==============

The original multiplexing logger implementation written in Python. To use make
sure you have the Python tool "virtualenv" installed on your system:

    virtualenv .
    . bin/activate
    pip install mmstats
    python python_logd.py python_logd.ini

punish_logd.py
==============

Simple script to connect to the socket created by either logging daemon
implementation and throw sample log messages at it. No dependencies should be
required other than Python 2.6+.

    # Fork 2 processes and have each send 10k messages to golog.sock
    python punish_logd.py 2 10000 golog.sock
    # Fork 3 processes, each sends 10k messages, and send SIGHUP a few times
    python punish_logd.py 3 10000 python_log.sock $(cat python_logd.pid)
