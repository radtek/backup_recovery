#!/usr/bin/env python 
# coding=UTF-8

import sys
from optparse import OptionParser 
import os
import re
from subprocess import Popen, PIPE
import time, logging,subprocess

#python oraclerestore_auto.py -d bpm -C BLEGRCT001 -S SYNBUmaster  -e '2019-06-26 12:00:00'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
log_path = os.path.dirname(os.getcwd()) + '/Logs/'
log_name =rq + '.log'
logfile = log_name
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)   
formatter = logging.Formatter("auto_restore_debug: %(message)s")
# formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

parser = OptionParser()
def parse_cmdline(argv):  
    usage = "usage: %prog [options] arg"  
    parser = OptionParser(usage)  
    # 第一个参数  #数据库名称
    parser.add_option('-d', '--dbname', dest='dbname', action='store',
                      metavar="dbname", help='oracle dbname')

    # 第二个参数  #NBU Master
    parser.add_option('-S', '--nbumasterserver', dest='nbumasterserver', action='store',
                      metavar="nbumasterserver", help='nbumasterserver [-S master_server]')
    
    # 第三个参数  #NBU Client
    parser.add_option('-C', '--nbuclientserver', dest='nbuclientserver', action='store',
                      metavar="nbuclientserver", help='NBU client CLIENT_NAME')  
    
    # 第四个参数  #开始时间
#    parser.add_option('-s', '--begintime', dest='begintime', action='store',
#                      metavar="begintime", help='begintime [-s mm/dd/yyyy  [HH:MM:SS]]')
    
    # 第五个参数 #结束时间
    parser.add_option('-e', '--endtime', dest='endtime', action='store',
                       metavar="endtime", help='endtime [-e mm/dd/yyyy  [HH:MM:SS]]')
    
    (options, args) = parser.parse_args(args=argv[1:])
    return (options, args)

def main(argv):
    # 分析参数和选项
    logger.info('分析参数和选项---参数信息开骀')
    options, args = parse_cmdline(argv)
    logger.info(options)
    db_name = options.dbname
    nbuMasterServer = options.nbumasterserver
    nbuClientServer = options.nbuclientserver
#    begindata = options.begintime
#    begindataArray = time.strptime(begindata, "%Y-%m-%d %H:%M:%S")
#    begin = time.strftime("%m/%d/%Y %H:%M:%S", begindataArray)
    enddata = options.endtime
    enddataArray = time.strptime(enddata, "%Y-%m-%d %H:%M:%S")
    end = time.strftime("%m/%d/%Y %H:%M:%S", enddataArray)
    logger.info('分析参数和选项---参数信息结束')
    directoryValidate(nbuClientServer, nbuMasterServer, db_name, end)

def directoryValidate(nbuClientServer, nbuMasterServer, db_name, end):
    CheckBackupFile(nbuClientServer, nbuMasterServer, db_name, end)
    
def CheckBackupFile(nbuClientServer, nbuMasterServer, db_name, end):
    logger.info('检查控制文件----开始')
    print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
#     fileAll = os.popen('/usr/openv/netbackup/bin/bplist -S SYNBUmaster  -C ' + nbuClientServer + ' -t 4 -l -s ' + end + '  -e ' + end + ' -R / | awk \'{print $4,$5,$6,$7,$8}\'').read()
    fileNames = os.popen('/usr/openv/netbackup/bin/bplist -S ' + nbuMasterServer + ' -C ' + nbuClientServer + ' -t 4 -l -e ' + end + ' -R / | awk \'{print $8}\'').readlines()
    fileDate = os.popen('/usr/openv/netbackup/bin/bplist -S ' + nbuMasterServer + ' -C ' + nbuClientServer + ' -t 4 -l -e ' + end + '  -R / | awk \'{print $5,$6,$7}\'').read()
    fileSize = os.popen('/usr/openv/netbackup/bin/bplist -S ' + nbuMasterServer + ' -C ' + nbuClientServer + ' -t 4 -l -e ' + end + '  -R / | awk \'{print $4}\'').read()
    names = []
    for name in fileNames:
        temp = name.replace('/', '').replace('\x00\n', '')
        names.append(temp)
#     print(names)
    logger.info(names)
    todateArray = time.strptime(end, "%m/%d/%Y %H:%M:%S")
    tdate = time.strftime("%Y-%m-%d %H:%M:%S", todateArray)
    controlFileName=''
    for e in names:
        if e.startswith('control'):
            idx=names.index(e)
            if idx>0:
                preIdx = idx-1;
                pre = names[preIdx]
                if pre.startswith('dbfull'):
                    controlFileName =e
                    break;
    print(controlFileName)          

    print('--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
    logger.info('检查控制文件----结束')
    
    if names:
        RestoreControlFile(nbuClientServer, nbuMasterServer, db_name, controlFileName, end)
#        MountOtherDb(db_name)
#        RestoreDBFile(nbuClientServer, nbuMasterServer, db_name, end)
#        RecoverDB(nbuClientServer, nbuMasterServer, db_name, end)
#        OpenResetLogs(nbuClientServer, nbuMasterServer, db_name, end)
#        OpenOtherDb(db_name)
    else:
        exit()

# 还原控制文件   
def RestoreControlFile(nbuClientServer, nbuMasterServer, db_name, controlFileName, end):
    logger.info('还原控制文件----开始')
    logger.info('最新控制文件如下：' + controlFileName)
    
    restoreScript = """
    run{
    startup nomount;
    shutdown abort;
    startup nomount;
    allocate channel t1 type 'sbt_tape';
    send 'nb_ora_serv=""" + nbuMasterServer + """, nb_ora_client=""" + nbuClientServer + """';
    restore controlfile from '""" + controlFileName + """';
    release channel t1;
    alter database mount;
    }
    """
    f=open('controlfile.rman', 'w')
    f.write(restoreScript)
    f.close()
    new_env = os.environ.copy()
    new_env['ORACLE_SID'] = db_name
    Proc = subprocess.Popen(["rman", "target", "/","cmdfile=controlfile.rman","log=controlfile.log"], env=new_env, stdout=PIPE, stdin=PIPE, stderr=PIPE)
#    Proc.stdin.write(restoreScript)
    (out, err) = Proc.communicate()
    print('-----------------------')
    print(Proc.returncode)
    print('-----------------------')
    if Proc.returncode != 0:
        print('ControlFile还原失败')
        logger.info('ControlFile还原失败')
    else:
        print('ControlFile还原成功')
        logger.info('ControlFile还原成功')
    if Proc.returncode != 0:
        print(err)
        logger.info(err)
#        sys.exit(Proc.returncode)
    else:
        print(out)
        logger.info(out)

#对端数据库启动到mount阶段
def MountOtherDb(db_name):
    logger.info('对端数据库打开到mount阶段----开始')
    new_db_name=db_name.strip('1')+'2'
    mountdb = """
    startup mount
    """
    mproc = Popen(["sqlplus","sys/oracle@"+new_db_name,"as","sysdba"],stdin=PIPE,stdout=PIPE,stderr=PIPE)
    mproc.stdin.write(mountdb)
    (out,err)=mproc.communicate()
    if mproc.returncode != 0:
        print('对端数据库启动到mount阶段失败')
        logging.info('对端数据库启动到mount阶段失败')
    else:
        print('对端数据库启动到mount阶段成功')
        logging.info('对端数据库启动到mount阶段成功')
    if mproc.returncode != 0:
       print(err)
       logging.info(err)
    else:
       print(out)
       logging.info(out) 
    logger.info('对端数据库打开到mount阶段----结束')

# 还原数据文件
def RestoreDBFile(nbuClientServer, nbuMasterServer, db_name, end):
    logger.info('还原数据文件----开始') 
    db_new_name=db_name.strip('1')
    new_env = os.environ.copy()
    new_env['ORACLE_SID'] = db_name
    RestoreDB = """
    run{
    allocate channel ch00 type 'sbt_tape';
    allocate channel ch01 type 'sbt_tape';
    allocate channel ch02 type 'sbt_tape';
    allocate channel ch03 type 'sbt_tape';
    allocate channel ch04 type 'sbt_tape';
    send 'nb_ora_serv=""" + nbuMasterServer + """, nb_ora_client=""" + nbuClientServer + """';
    set newname for database to '+DATADG/""" + db_new_name + """/%b';
    set newname for tempfile 1 to '+DATADG/""" + db_new_name + """/%b';
    restore database;
    switch datafile all;
    switch tempfile all;
    release channel ch00;
    release channel ch01;
    release channel ch02;
    release channel ch03;
    release channel ch04;
    }
    """
    f=open('restore.rman', 'w')
    f.write(RestoreDB)
    f.close()
    new_env = os.environ.copy()
    new_env['ORACLE_SID'] = db_name
#     file = '/log/'+db_name + "/"+rq + '/restore.log'
    RmanProc = Popen(["rman" , "target" , "/" , "cmdfile=restore.rman" ,"log=restore.log"], env=new_env, stdout=PIPE, stdin=PIPE, stderr=PIPE)
#    RmanProc.stdin.write(RestoreDB)
    (out, err) = RmanProc.communicate()
    print('-----------------------')
    print(RmanProc.returncode)
    print('-----------------------')
    if RmanProc.returncode != 0:
        print('Restore还原失败')
        logger.info('Restore还原失败')
    else:
        print('Restore还原成功')
        logger.info('Restore还原成功')
    if RmanProc.returncode != 0:
        print (err)
        logger.info(err)
#        sys.exit(RmanProc.returncode)
    else:
        print (out)
        logger.info(out)
        
    logger.info('还原数据文件----结束')

#恢复数据库
def RecoverDB(nbuClientServer, nbuMasterServer, db_name, end): 
    logger.info('recoverdb----开始')
    todateArray = time.strptime(end, "%m/%d/%Y %H:%M:%S")
    tdate = time.strftime("%Y-%m-%d %H:%M:%S", todateArray)
    print(tdate)
    timeArray = time.strptime(tdate, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    print(timeStamp)
    timestamp2 = timeStamp - 43200
    time_tuples = time.localtime(timestamp2)
    recoverdate = time.strftime('%Y-%m-%d %H:%M:%S', time_tuples)
    print(recoverdate)

 
    recoverScript = """
    run {
    allocate channel t1 type 'sbt_tape' parms 'ENV=(NB_ORA_CLIENT=""" + nbuClientServer + """,NB_ORA_SERV=""" + nbuMasterServer + """)';
    set until time= "to_date('""" + recoverdate + """','yyyy-mm-dd hh24:mi:ss')";
    recover database;
    release channel t1;
    } 
    """
    f=open('recover.rman', 'w')
    f.write(recoverScript)
    f.close()
    new_env = os.environ.copy()
    new_env['ORACLE_SID'] = db_name
    ProgressProc = Popen(["rman" , "target", "/" ,"cmdfile=recover.rman" , "log=recover.log"], env=new_env, stdout=PIPE, stdin=PIPE, stderr=PIPE)
#    ProgressProc.stdin.write(recoverScript)
    (out, err) = ProgressProc.communicate()
    print('-----------------------')
    print(ProgressProc.returncode)
    print('-----------------------')
    if ProgressProc.returncode != 0:
        print('Recover还原失败')
        logger.info('Recover还原失败')
    else:
        print('Recover还原成功')
        logger.info('Recover还原成功')
    
    if ProgressProc.returncode != 0:
        print (err)
        logger.info(err)
#        sys.exit(ProgressProc.returncode)
    else:
        print (out)
        logger.info(out)
    logger.info('recoverdb----结束')

#resetlog打开数据库
def OpenResetLogs(nbuClientServer, nbuMasterServer, db_name, end):
    print('使用openresetlogs打开数据库')
    logger.info('使用openresetlogs打开数据库----开始')
    OpenDB = """
    set linesize 999
    alter database open resetlogs;
    """
    new_env = os.environ.copy()
    new_env['ORACLE_SID'] = db_name
    OpenResetlogsProc = Popen(["sqlplus", "-S", "/", "as", "sysdba"], env=new_env, stdout=PIPE, stdin=PIPE, stderr=PIPE)
    OpenResetlogsProc.stdin.write(OpenDB)
    (out, err) = OpenResetlogsProc.communicate()
    if OpenResetlogsProc.returncode != 0:
        print (err)
        logger.info(err)
#        sys.exit(OpenResetlogsProc.returncode)
    else:
        print (out)
        logger.info(out)
    print('')
    print('=====数据库恢复结束=====')
    print('')
    logger.info('使用openresetlogs打开数据库----结束')

#对端数据库open打开
def OpenOtherDb(db_name):
    logger.info('对端数据库open----开始')
    new_db_name=db_name.strip('1')+'2'
    opendb = """
    alter database open;
    """
    mproc = Popen(["sqlplus","sys/oracle@"+new_db_name,"as","sysdba"],stdin=PIPE,stdout=PIPE,stderr=PIPE)
    mproc.stdin.write(opendb)
    (out,err)=mproc.communicate()
    if mproc.returncode != 0:
        print('对端数据库open失败')
        logging.info('对端数据库open失败')
    else:
        print('对端数据库open成功')
        logging.info('对端数据库open成功')
    if mproc.returncode != 0:
       print(err)
       logging.info(err)
    else:
       print(out)
       logging.info(out) 
    logger.info('对端数据库open----结束')

if __name__ == "__main__":
    main(sys.argv)
