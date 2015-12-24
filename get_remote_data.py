#!/usr/bin/python
# coding: utf-8
#
# crawler_industry_text
# $Id: crawler_industry_text.py  2015-12-14 Qiu $
#
# history:
# 2015-12-14 Qiu   created

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

import os
import paramiko
import re
import time


class GetRemoteDataFile(object):

    def __init__(self):

        remote_ip = '120.55.189.211'
        user = 'root'
        passwd = '29fVmC@u'
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(remote_ip, 22, user, passwd)
        self.transport = paramiko.Transport((remote_ip, 22))
        self.transport.connect(username=user, password=passwd)
        self.remote_path_list = [r'/home/jsdx/', r'/home/zjdx/']
        self.local_path = r'/home/hadoop/data_files/'

    def get_files_list(self, remote_path):

        stdin, stdout, stderr = self.ssh.exec_command("ls %s" % remote_path)
        if not stderr.readlines():
            remote_file = stdout.readlines()
        result = []
        if remote_file:
            for line in remote_file:
                grep_zjdx = re.search(r'^kunyan_\d{10}$', line)
                grep_jsdx = re.search(r'^jsdx_\d{10}$', line)
                grep = grep_jsdx or grep_zjdx
                if grep:
                    result.append(line.encode().split()[0])
            return result
        else:
            print "Remote file empty"

    def download_file(self, file_name, remote_path, local_path):
        """download remote file.


        Attributes:
            file_name:file name.
        """
        sftp = paramiko.SFTPClient.from_transport(self.transport)
        sftp.get(remote_path+file_name, local_path+file_name)

    def just_do_it(self, remote_path, local_path):

        """do it.


        Attributes:
            no.
        """
        remote_list = self.get_files_list(remote_path)
        if remote_list:
            for line in remote_list:
                self.download_file(line, remote_path, local_path)
                all_files = os.listdir(local_path)
                local_file_list = []
                for file_name in all_files:
                    result_1 = re.search(r'^kunyan_\d{10}$', file_name)
                    result_2 = re.search(r'^jsdx_\d{10}$', file_name)
                    result = result_1 or result_2
                    if result:
                        local_file_list.append(file_name)
                if line in local_file_list:
                    stdin, stdout, stderr = self.ssh.exec_command("mv -f %s /home/dx_datafile" % (remote_path+line))
                    print "remote data file '%s' has been downloaded successfully" % line
                    if not stderr.readlines():
                        print "remote files '%s' has been moved successfully" % line

    def main(self):

        """main function.


        Attributes:
            no.
        """
        log_time = time.strftime('%Y-%m-%d %X', time.localtime())
        log = open('/home/hadoop/log', 'a+')
        log.write('Begin at '+log_time+'\n')
        print 'Begin at '+log_time+'\n'
        for path in self.remote_path_list:
            self.just_do_it(path, self.local_path)
        log_time = time.strftime('%Y-%m-%d %X', time.localtime())
        log.write('Begin at '+log_time+'\n')
        print 'End at '+log_time+'\n'+'\n'
        log.close()
        self.ssh.close()
        self.transport.close()

if __name__ == '__main__':
    get_remote_data_file = GetRemoteDataFile()
    get_remote_data_file.main()
