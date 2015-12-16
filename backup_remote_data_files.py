#!/usr/bin/python
#coding: utf-8
#
# backup_remote_data_files
# $Id: backup_remote_data_files.py  2015-09-30 Qiu $
#
# history:
# 2015-10-16 Qiu   created
#
# wangQiu@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# backup_remote_data_files.py is
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
backup_remote_data_files

Back up remote data files to local

run on 192.168.0.2
"""

import os
import paramiko
import re
import time


class BackupRemote(object):

    """Back up remote data files to local.


    Attributes:
        no.
    """

    def __init__(self):
        """initiate object.


        Attributes:
            no.
        """
        remote_ip = '120.55.189.211'
        user = 'root'
        password = '29fVmC@u'
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(remote_ip, 22, user, password)
        self.transport = paramiko.Transport((remote_ip, 22))
        self.transport.connect(username=user, password=password)
        self.remote_path_list = ['/home/zjdx/unbacked_redis_files/', '/home/jsdx/unbacked_redis_files/']
        self.local_path_list = ['/home/hadoop/zjdxdata_files/', '/home/hadoop/jsdxdata_files/']

    def get_files_list(self, remote_path):

        """get remote kunyan data file list.


        Attributes:
            remote_path:remote path.
        """
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

    # def delete_file(self, file_name):
    #     """delete remote file.
    #
    #
    #     Attributes:
    #         file_name:file name.
    #     """
    #     self.ssh.exec_command("rm -f %s" % (self.remote_path+file_name))

    def just_do_it(self, remote_path, local_path):
        """main function.


        Attributes:
            no.
        """
        log_time = time.strftime('%Y-%m-%d %X', time.localtime())
        log = open('/home/hadoop/log', 'a+')
        log.write('Begin at '+log_time+'\n')
        print 'Begin at '+log_time+'\n'
        remote_list = self.get_files_list(remote_path)
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
                stdin, stdout, stderr = self.ssh.exec_command("rm -f %s" % (remote_path+line))
                print "remote data file '%s' has been downloaded successfully" % line
                if not stderr.readlines():
                    print "remote files '%s' has been deleted successfully" % line
        self.ssh.close()
        self.transport.close()
        log_time = time.strftime('%Y-%m-%d %X', time.localtime())
        log.write('Begin at '+log_time+'\n')
        print 'End at '+log_time+'\n'+'\n'
        log.close()

if __name__ == '__main__':
    back_up = BackupRemote()
    back_up.main()
