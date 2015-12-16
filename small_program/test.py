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
# import time


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
        password = 'Dataservice2015'
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(remote_ip, 22, user, password)
        self.transport = paramiko.Transport((remote_ip, 22))
        self.transport.connect(username=user, password=password)
        self.remote_path = '/home/zjdx/unbacked_redis_files/'
        self.local_path = 'D:/home/hadoop/zjdxdata_files/'

    def get_files_list(self, remote_path):
        """get remote kunyan data file list.


        Attributes:
            remote_path:remote path.
        """
        stdin, stdout, stderr = self.ssh.exec_command("ls %s" % remote_path)
        if not stderr.readlines():
            remote_file = stdout.readlines()
        result = []
        print result
        if remote_file:
            for line in remote_file:
                grep = re.search(r'^kunyan_\d{10}$', line)
                if grep:
                    result.append(line.encode().split()[0])
                return result
        else:
            print "Remote file empty"

    def download_file(self, file_name):
        """download remote file.


        Attributes:
            file_name:file name.
        """
        sftp = paramiko.SFTPClient.from_transport(self.transport)
        sftp.get(self.remote_path+file_name, self.local_path+file_name)

    # def delete_file(self, file_name):
    #     """delete remote file.
    #
    #
    #     Attributes:
    #         file_name:file name.
    #     """
    #     self.ssh.exec_command("rm -f %s" % (self.remote_path+file_name))

    def main(self):
        """main function.


        Attributes:
            no.
        """
        remote_list = self.get_files_list(self.remote_path)
        print remote_list
        # for line in remote_list:
        #     self.download_file(line)
        #     all_files = os.listdir(self.local_path)
        #     local_file_list = []
        #     for file_name in all_files:
        #         result = re.search(r'^kunyan_\d{10}$', file_name)
        #         if result:
        #             local_file_list.append(file_name)
        #     if line in local_file_list:
        #         # stdin, stdout, stderr = self.ssh.exec_command("rm -f %s" % (self.remote_path+line))
        #         print "remote data file '%s' has been downloaded successfully" % line
        #         # if not stderr.readlines():
        #         print "remote files '%s' has been deleted successfully" % line
        self.ssh.close()
        self.transport.close()

if __name__ == '__main__':
    back_up = BackupRemote()
    back_up.main()
