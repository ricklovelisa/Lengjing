#!/usr/bin/python
#coding: utf-8
#
# send_sms
# $Id: send_sms.py  2015-09-30 Haitao $
#
# history:
# 2015-10-09 Haitao   created
# 2015-10-09 Haitao   modified
# 2015-10-12 Kerry    modified
#
# wanghaitao@kunyan-inc.com
# http://www.kunyandata.com
#
# --------------------------------------------------------------------
# send_sms.py is
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
send_sms.py

Send message.
"""


import re
import urllib2


class SendSms(object):

    """Send message.

    Send mobele message.

    Attributes:
        mobile: mobele phone number.
        content: message to be sent.
    """

    def __init__(self):
        """inition for LoadData.

        Args:
            no
        """
        self.userid = '66'
        self.account = 'HY_KunYanData'
        self.password = '98349823984'


    def _run(self,phone, message):
        """run function.

        Args:
            no
        """
        url = 'http://115.29.49.158:8888/sms.aspx?' \
                   'action=send&userid=%s&account=%s' \
                   '&password=%s&mobile=%s&content=%s' \
                   % (self.userid, self.account, \
                   self.password, phone, message)
        
        response = urllib2.urlopen(url)
        data = response.read()
        
        result = re.search(r'Success',data)
        if result:
            print "message sent success!"
            return True
        else:
            print "message sent faild!"
            return False

    def main(self, phone, message):
        """Main function.

        Args:
            no
        """
        return self._run(phone, message)

if __name__ == '__main__':
    """Main function.

        Args:
            no
   """
    
    sms = SendSms()
    sms.main('15868154558,15158114927', '【技术平台】数据平台已经崩溃')
    sms.main('15158114927,15868154558', '【计算平台】计算平台已经崩溃')
    