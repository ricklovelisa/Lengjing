#!/usr/bin/python
#coding: utf-8
#
# visit_and_search_linux
# $Id: visit_and_search_linux.py  2015-09-30 Haitao $
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
#visit_and_search_linux.py is
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
visit_and_search_linux.py

Run zjdx_data_linux.py and back_redis.py per SECONDS_PER_DELTA times
"""

import threading
import time
import zjdx_data_linux
import back_redis


def say_hello():
    """Run zjdx_data_linux.py and back_redis.py per SECONDS_PER_DELTA times.

        Args:
            no
        """
    print 'hello' + time.strftime('%Y-%m-%d %H:%M:%S')
    timer_log = open('/home/zjdx/timer_log', 'a+')
    timer_log_time = time.strftime('%Y-%m-%d %H:%M:%S')
    timer_log.write(timer_log_time + "\n")
    timer_log.close()
    zjdx = zjdx_data_linux.ZjdxDataLinux()
    zjdx.main()
    backup_redis = back_redis.BackRedis()
    backup_redis.main()
    global time_thread       #Notice: use global variable!
    SECONDS_PER_DELTA = 30 * 60
    time_thread = threading.Timer(SECONDS_PER_DELTA, say_hello)
    time_thread.start()


def main():
    """Main function.

        Args:
            no
   """
    time_thread = threading.Timer(1.0, say_hello)
    time_thread.start()

if __name__ == '__main__':
    """Main function.

        Args:
            no
   """
    main()
