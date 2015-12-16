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
import load_data
import time

class MultipleLoad(object):

    """Multiple load data

    Load  stock data  time to redis.

    Attributes:
        input_file: input data..
    """

    def __init__(self, path):

        """initiate class

        Load  stock data  time to redis.

        Attributes:
            input_file: input data..
        """
        self._file = path
        self._k = 500000


    def _counts(self, file):

        """initiate class

        Load  stock data  time to redis.

        Attributes:
            file: data file
        """
        i = 0
        for line in file:
            i += 1
        file.seek(0)
        return i

    def _get_seg_ID(self, row_nums, k):

        """initiate class

        Get start ID of each block.

        Attributes:
            n: the row nums of file
            k: the num of loading data every time.
        """
        n = row_nums/k
        result = {}
        for i in range(0, n+1):
            result[i] = (i*k, (i+1)*k)
        return result

    # def _add_data(self, target_dict, empty_dict, key, line, counter):
    #
    #     """add data
    #
    #     Load  stock data  time to redis.
    #
    #     Attributes:
    #         path:file path
    #         k: the num of loading data every time.
    #     """
    #     head = target_dict.has_key(key-1)
    #     end = target_dict.has_key(key+1)
    #     if head & end:
    #         if target_dict[key-1] < counter <=  target_dict[key]:
    #             empty_dict[key].append(line)
    #

    def _seg_file(self, path, k):

        """seg files

        Load  stock data  time to redis.

        Attributes:
            path:file path
            k: the num of loading data every time.
        """
        file = open(path, 'r')
        row_nums = self._counts(file)
        seg_ID = self._get_seg_ID(row_nums, k)
        data = {}
        for key in seg_ID:
            data[key] = []
        counter = 0
        for line in file:
            for key in seg_ID:
                head = seg_ID[key][0]
                end = seg_ID[key][1]
                if head < counter < end:
                    data[key].append(line)
            counter += 1
        return data

    def main(self):

        """main function

        Load  stock data  time to redis.

        Attributes:
            no.
        """

        thread_pools = []
        data = self._seg_file(self._file, self._k)
        for key in data:
            Load = load_data.LoadData(data[key])
            start = time.time()
            thr = threading.Thread(target=Load.main())
            thread_pools.append(thr)
            end = time.time()
            print end - start
        start = time.time()
        for t in thread_pools:
            t.start()
        for t in thread_pools:
            t.join()
        end = time.time()
        print end - start


if __name__ == '__main__':
    multiple_load = MultipleLoad('D:/home/jsdx/jsdx_2015111313')
    multiple_load.main()

