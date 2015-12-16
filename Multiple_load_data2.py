#!/usr/bin/python
#coding: utf-8
#
# multiple load data to redis
# $Id: Multiple_load_data.py  2015-11-30 Qiu $
#
# history:
# 2015-11-30 Qiu   created

# qiuqiu@kunyand-inc.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
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
Multiple_load_data.py
"""

import threading
import time
from load_data import LoadData

class MultipleLoad(object):
    """
    Multiple load data
    Load  stock data  time to redis.
    Attributes:
        input_file: input data..
    """

    def __init__(self, path):
        """
        initiate class
        Load  stock data  time to redis.
        Attributes:
            input_file: input data..
        """
        self._file = path
        self._k = 500000

    def _get_seg_ID(self, row_nums, k):
        """
        initiate class
        Get start and end ID of each block.
        Attributes:
            n: the row nums of file
            k: the num of loading data every time.
        """
        n = row_nums/k
        result = {}
        for i in range(0, n+1):
            if i==n:
                end = row_nums
            else:
                end = (i+1)*k
            result[i] = (i*k, end)
        return result

    def _seg_file(self, path, k):
        """
        seg files
        Load  stock data  time to redis.
        Attributes:
            path:file path
            k: the num of loading data every time.
        """
        with open(path, 'r') as f:
            lines = f.readlines()
            seg_ID = self._get_seg_ID(len(lines), k)
            data = {}
            for key in seg_ID.keys():
                head = seg_ID[key][0]
                end = seg_ID[key][1]
                data[key] = lines[head:end]
        return data

    def main(self):
        """
        main function
        Load  stock data  time to redis.
        Attributes:
            no.
        """
        start = time.time()
        thread_pools = []
        data = self._seg_file(self._file, self._k)
        for key in data.keys():
            thr = threading.Thread(target=self._do, args=(data[key],))
            thread_pools.append(thr)
        for t in thread_pools:
            t.start()
        for t in thread_pools:
            t.join()
        end = time.time()
        print end - start

    def _do(self, data):
        load_data = LoadData(data)
        load_data.main()
        
if __name__ == '__main__':
    multiple_load = MultipleLoad('D:/home/jsdx/jsdx_2015111313')
    multiple_load.main()

