#!/usr/bin/python
#coding:utf-8

import paramiko

class host:
    def __init__(self, ip, port, user, passwd):
        self._ip = ip
        self._port = port
        self._user = user
        self._passwd = passwd


    # 判断以userid为名称的目录是否已经存在
    def path_exists(self, userid):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self._ip, int(self._port), username=self._user, password=self._passwd)
            sftp = ssh.open_sftp()
            sftp.stat("/mnt/file-storage/platform/{userid}".format(userid=userid))
            print("exist")
            ssh.close()
            return True
        except IOError:
            print("not exist")
            return False

    # 以userid为名称，创建目录
    def mkdir(self, userid):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self._ip, int(self._port), username=self._user, password=self._passwd)
            sftp = ssh.open_sftp()
            sftp.mkdir("/mnt/file-storage/platform/{userid}".format(userid=userid))
            print("Create forld %s in remote hosts successfully!\n" % userid)
            ssh.close()
            return True
        except:
            print("Failed to create folder!\n")
            return False

    # 执行远程命令
    def run(self, cmd):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self._ip, int(self._port), username=self._user, password=self._passwd)
            for m in cmd:
                stdin, stdout, stderr = ssh.exec_command(m)
                # print(stdout.read())
                # print("Check Status: %s\tOK\n" % (self._ip))
            ssh.close()
            return True
        except:
            print("%s\tError\n" % (self._ip))
            return False



def nfs_dir_create(userid):
    info = host("ip", "port",  "user", "passwd")

    path_exists = info.path_exists(userid)
    if not path_exists:
        info.mkdir(userid)
        cmd = ["cp -r /mnt/file-storage/platform/tom/* /mnt/file-storage/platform/{userid}".format(userid=userid)]
        info.run(cmd)
        return True

if __name__ == '__main__':
    nfs_dir_create("lucy")