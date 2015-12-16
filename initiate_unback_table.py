#!/usr/bin/python
#coding: utf-8
#
# back_redis_modify
# $Id: initiate_unback_table.py  2015-10-20 Qiu $
#
# history:
# 2015-10-20 Qiu   created
#
# wangQiu@kunyandata.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# initiate_unback_table.py is
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
initiate_unback_table.py

initiate unback_redis_data table
"""

import MySQLdb
import paramiko
import os
import sys
import re

class InitTable(object):

    """initate table unbacked_redis_data in mysql.



    Attributes:
        no.
    """

    def __init__(self):

        """initate table unbacked_redis_data in mysql.



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
        self.remote_path = '/home/zjdx/'
        self.local_path = '/home/hadoop/zjdx'
        self.conn = MySQLdb.connect(host=remote_ip, user=user,
                                    passwd='hadoop', db='stock')
        self.curcor = self.conn.cursor()

    def get_files_list(self):
        """get remote file list.


        Attributes:
            remote_path:remote path.
        """
        stdin, stdout, stderr = self.ssh.exec_command("ls %s" % self.remote_path)
        if not stderr.readlines():
            remote_file = stdout.readlines()
        files = []
        for line in remote_file:
            files.append(line.encode().split()[0])
        return files

    def get_data_files_list(self):

        """get remote kunyan data file list.


        Attributes:
            remote_path:remote path.
        """
        files = self.get_files_list()
        result = []
        for line in files:
            rep = re.search(r'^kunyan_\d{10}$', line)
            if rep:
                result.append(line)
        return result

    def get_file_comment(self):

        """get redis data list in 'files'.



        Attributes:
            no.
        """

        remote_file_list = self.get_files_list()
        if 'files' in remote_file_list:
            sftp = paramiko.SFTPClient.from_transport(self.transport)
            sftp.get(self.remote_path+'files', self.local_path+'files')
        isexists = os.path.exists(self.local_path+"files")
        if isexists:
            file_comment = open(self.local_path+"files")
            result = file_comment.readlines()
            results = []
            for line in result:
                results.append(line.split("\n")[0])
            return results
        else:
            print "Sorry, no file named 'files'."

    def create_table(self):

        """create table unbacked_redis_data.



        Attributes:
            no.
        """
        create_sql = "CREATE TABLE unbacked_redis_data " \
              "(unbacked_redis_file VARCHAR(50))"
        try:
            self.curcor.execute(create_sql)
            self.conn.commit()
        except Exception, e:
            print e

    def get_unbacked_list(self):

        """get unbacked redis file list.



        Attributes:
            no.
        """
        try:
            self.curcor.execute("SELECT v_hour FROM backed_hours GROUP BY v_hour")
        except Exception, e:
            print e
        backed_hours = self.curcor.fetchall()
        backed_hour_list = []
        for line in backed_hours:
            backed_hour_list.append(line[0])
        file_comment = self.get_file_comment()
        time = []
        if file_comment:
            for line in file_comment:
                time_file = line[7:11]+'-'+line[11:13]+ '-'\
                       +line[13:15]+' '+line[15:17]
                if time_file not in backed_hour_list:
                    time.append(time_file)
            return time
        else:
            print "All files have been backed"
            return None

    def down_load_files(self):

        """down load files from remote.



        Attributes:
            no.
        """
        data_files_list = self.get_file_comment()
        remote_file_list = self.get_files_list()
        unbacked_list = self.get_unbacked_list()
        isexist = os.path.exists(self.local_path+'data_files/')
        if not isexist:
            os.mkdir(self.local_path+'data_files/')
        if unbacked_list:
            sftp = paramiko.SFTPClient.from_transport(self.transport)
            print "begining to move data files"
            j = ''
            i = 1.0
            length = len(remote_file_list)
            last_index = 0
            for line in data_files_list:
                if line not in remote_file_list:
                    continue
                sftp.get(self.remote_path+line, self.local_path+'data_files/'+line)
                local = os.path.exists(self.local_path+'data_files/'+line)
                if local:
                    self.ssh.exec_command("rm -f %s" % (self.remote_path+line))
                index = int((i/length)*100)
                sys.stdout.write(str(index)+'% ||'+j+'->'+"\r")
                sys.stdout.flush()
                if index > last_index:
                    j += '#'
                i += 1.0
                last_index = index
            print "move files end"

    def main(self):

        """main function.



        Attributes:
            no.
        """
        self.down_load_files()
        unbacked_list = self.get_unbacked_list()
        if unbacked_list:
            try:
                self.curcor.execute("SHOW TABLES IN `stock`")
            except Exception, e:
                print e
            table = self.curcor.fetchall()
            tables = []
            if "unbacked_redis_data" not in tables:
                self.create_table()
            for item in table:
                tables.append(item[0])
            for line in unbacked_list:
                try:
                    self.curcor.execute("INSERT INTO unbacked_redis_data SET "
                                        "unbacked_redis_file = '%s'" % line)
                    self.conn.commit()
                except Exception, e:
                    print e

if __name__ == '__main__':
    inti_table = InitTable()
    inti_table.main()
