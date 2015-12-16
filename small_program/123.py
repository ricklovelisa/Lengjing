import paramiko
import MySQLdb
import os

remote_ip = '120.55.189.211'
user = 'root'
password = 'Dataservice2015'
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(remote_ip, 22, user, password)
transport = paramiko.Transport((remote_ip, 22))
transport.connect(username=user, password=password)
remote_path = '/home/wht/'
local_path = 'D:/home/'
conn = MySQLdb.connect(host='192.168.0.33', user='root',
                       passwd='root', db='stock')
curcor = conn.cursor()

def get_files_list(remote_path):
        """get remote kunyan data file list.


        Attributes:
            remote_path:remote path.
        """
        stdin, stdout, stderr = ssh.exec_command("ls %s" % remote_path)
        if not stderr.readlines():
            remote_file = stdout.readlines()
        result = []
        for line in remote_file:
            result.append(line.encode().split()[0])
        return result

def get_file_comment():

        """get redis data list in 'files'.



        Attributes:
            no.
        """

        remote_file_list = get_files_list(remote_path)
        if 'files' in remote_file_list:
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.get(remote_path+'files', local_path+'files')
        isexists = os.path.exists(local_path+"files")
        if isexists:
            file_comment = open(local_path+"files")
            result = file_comment.readlines()
            results = []
            for line in result:
                results.append(line.split("\n")[0])
            return results
        else:
            print "Sorry, no file named 'files'."



def get_unbacked_list():

        """get unbacked redis file list.



        Attributes:
            no.
        """
        try:
            curcor.execute("SELECT v_hour FROM backed_hours GROUP BY v_hour")
        except Exception, e:
            print e
        backed_hours = curcor.fetchall()
        backed_hour_list = []
        for line in backed_hours:
            backed_hour_list.append(line[0])
        file_comment = get_file_comment()
        time = []
        if file_comment:
            for line in file_comment:
                t = line[7:11]+'-'+line[11:13]+ '-'\
                       +line[13:15]+' '+line[15:17]
                if t not in backed_hour_list:
                    time.append(t)
            return time
        else:
            return None
            print "All files have been backed"

for line in result:
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get(remote_path+line, local_path+line)
