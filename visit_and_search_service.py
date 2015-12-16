#!/usr/bin/python
#coding: utf-8
#
# visit_and_search_service
# $Id: visit_and_search_service.py  2015-09-30 Haitao $
#
# history:
# 2015-09-30 Haitao   created
# 2015-09-30 Haitao   modified
# 2015-10-08 Haitao   modified
#
# wanghaitao@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# visit_and_search_service.py is
#
# Copyright (c)  by ShangHai KunYan Data Service Co. Ltd..  All rights reserved.
#
# By obtaining, using, and/or copying this software and/or its
# associated documentation, you agree that you have read, understood,
# and will comply with the following terms and conditions:
#
# Permission to use, copy, modify, and distribute this software and
# its associated documentation for any purpose and without fee is
# hereby granted, provided that the above copyright notice appears in
# all copies, and that both that copyright notice and this permission
# notice appear in supporting documentation, and that the name of
# ShangHai KunYan Data Service Co. Ltd. or the author
# not be used in advertising or publicity
# pertaining to distribution of the software without specific, written
# prior permission.
#
# --------------------------------------------------------------------

"""
visit_and_search_service.py

Set visit_and_search_linux as linux system service
"""

import sys
import os
import visit_and_search_linux


def daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    """Set visit_and_search_linux as linux system service

        Args:
            no
    """
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)
    os.chdir("/")
    os.umask(0)
    os.setsid()
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)   #第二个父进程退出
    except OSError, e:
        sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    for file_line in sys.stdout, sys.stderr:
        file_line.flush()
    std_in = open(stdin, 'r')
    std_out = open(stdout, 'a+')
    std_err = open(stderr, 'a+', 0)
    os.dup2(std_in.fileno(), sys.stdin.fileno())
    os.dup2(std_out.fileno(), sys.stdout.fileno())
    os.dup2(std_err.fileno(), sys.stderr.fileno())


def main():
    """Main function.

        Args:
            no
    """
    sys.stdout.write('Daemon started with pid %d\n' % os.getpid())
    sys.stdout.write('Daemon stdout output\n')
    sys.stderr.write('Daemon stderr output\n')
    visit_and_search_linux.main()


if __name__ == "__main__":
    """Main function.

        Args:
            no
    """
    daemonize('/dev/null', '/tmp/daemon_stdout.log', '/tmp/daemon_error.log')
    main()
