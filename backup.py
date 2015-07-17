#!/usr/bin/env python

import os
import sys
import time
from subprocess import Popen, PIPE, STDOUT

INNOBACKUPEX = '/home/downloads/percona-xtrabackup-2.2.10-Linux-x86_64/bin/innobackupex'
SUCCESS_FLAG = 'completed OK!'
backup_type = 'snapshot'
def main():
    timestamp = time.strftime('%Y%m%d%H%M%S')
    backup_dir = '/home/backup/'
    target = os.path.join(backup_dir, timestamp)
    backup_info = os.path.join(target, 'backup.info')
    log_file = os.path.join(target, 'xtrabackup.log')

    start = time.time()
    cmd = [
        INNOBACKUPEX,
        '--user',
        'root',
        '--password',
#        '***********',
        '--port',
        '3306',
        '--slave-info',
        '-no-timestamp',
        '--socket',
        '/tmp/mysql.sock',
    ]
    cmd += [target]
    print cmd
    proc = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    for i in xrange(5):
        if os.path.exists(target):
            break
        else:
            time.sleep(1)
    with open(log_file, 'w') as f:
        for line in iter(proc.stdout.readline, ''):
            sys.stdout.write(line)
            f.write(line)
    proc.wait()
    with open(backup_info, 'w') as f:
        f.write('[info]\n')
    output = open(log_file).read()
    if not SUCCESS_FLAG in output.strip().splitlines()[-1]:
        with open(backup_info, 'a') as f:
            f.write('backup = fail\n')
        print >>sys.stderr, 'Backup failed:'
        print >>sys.stdout, output
        return 1
    else:
        with open(backup_info, 'a') as f:
            f.write('backup = success\n')
            f.write('backup_time = %ds\n' % (time.time() - start))

    start = time.time()
    print start
    print 'start apply-log..............'
    cmd = [
        INNOBACKUPEX,
        '--use-memory',
        '128MB',
        '--apply-log',
        '--redo-only',
        target
    ]   
    log_file = os.path.join(target, 'apply_log.log')
    proc = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with open(log_file, 'w') as f:
        for line in iter(proc.stdout.readline, ''):
            sys.stdout.write(line)
            f.write(line)
    proc.wait()
    print 'end apply-log..............'
    output = open(log_file).read()
    if not SUCCESS_FLAG in output.strip().splitlines()[-1]:
        with open(backup_info, 'a') as f:
            f.write('apply_log = fail\n')
        print >>sys.stderr, 'Apply log failed:'
        print >>sys.stdout, output
        return 1
    else:
        with open(backup_info, 'a') as f:
            f.write('apply_log = success\n')
            f.write('apply_log_time = %ds\n' % (time.time() - start))

    slave_info = os.path.join(target, 'xtrabackup_slave_info')

    return 0

if __name__ == '__main__':
    main()
