import re
import paramiko
import os
import MySQLdb

base_dir = 'D:/home/hadoop/wht/zjdx_backed/'
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("120.55.189.211", 22, "root", "Dataservice2015")
transport = paramiko.Transport(("120.55.189.211", 22))
transport.connect(username="root", password="Dataservice2015")
sftp = paramiko.SFTPClient.from_transport(transport)
remote_dir = '/home/wht/'
conn = MySQLdb.connect(host='192.168.0.26', user='root',
                               passwd='root', db='stock')
global cursor
cursor = conn.cursor()


def get_remote_file_list(remote_dir):
        """get remote file list.

        Args:
            remote_dir: remote file dir
        """
        remote_file_list = []
        stdin, stdout, stderr = ssh.exec_command("ls %s" %remote_dir)
        if stderr:
            remote_file = stdout.readlines()
        for file_name in remote_file:
            result = re.search(r'^kunyan_\d{10}$', file_name)
            if result:
                remote_file_list.append(file_name.encode().split()[0])
        return remote_file_list

def download_remote_data(file_name):
    """download remote data & remove remote data.

    Args:
            file_name: file name
    """
    sftp.get(remote_dir+file_name, base_dir+file_name)
    local_file_list = []
    all_file = os.listdir(base_dir)
    for file_name in all_file:
        result = re.search(r'^kunyan_\d{10}$', file_name)
        if result:
            local_file_list.append(file_name)
    if file_name in local_file_list:
        ssh.exec_command("rm -f %s" % (remote_dir+file_name))

def insert_to_mysql(file_name):
    """insert unbacked redis data into mysql.

     Args:
         no
    """
    sql_insert = "INSERT INTO unbacked_redis_data(unbacked_redis_stock)" \
                     " VALUES ('%s')" % file_name
    try:
        cursor.execute(sql_insert)
    except Exception, e:
        print e

remote_file_list = get_remote_file_list(remote_dir)
for line in remote_file_list:
    print line
    download_remote_data(line)
    insert_to_mysql(line)
    # file_new = open(base_dir+'files', 'a+')        
    # file_new.write(line)        
    # file_new.write('\n')      
    # file_new.close()
conn.commit()

print get_remote_file_list(remote_dir)