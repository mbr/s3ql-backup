#!/usr/bin/env python

import argparse
from datetime import datetime
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

class SignalException(Exception):
    def __str__(self):
        return 'Received signal %d' % self.args


def on_signal_raise(signum, frame):
    raise SignalException(signum)


class SignalMask(object):
    def __init__(self, handler, signals=None):
        if not signals:
            signals = set((signum for name, signum in signal.__dict__.iteritems()\
                              if name.startswith('SIG') and not\
                                 name.startswith('SIG_') and\
                                 int == type(signum)))
            # remove invalid signals
            signals.remove(signal.SIGKILL)
            signals.remove(signal.SIGSTOP)
        elif int == type(signals):
            signals = [signals]


        self.signals = signals
        self.handler = handler
        self.prev_handlers = {}

    def __enter__(self):
        for signum in self.signals:
            self.prev_handlers[signum] = signal.signal(signum, self.handler)

    def __exit__(self, type, value, traceback):
        for signum, handler in self.prev_handlers.iteritems():
            signal.signal(signum, handler)


class TempDir(object):
    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs

    def __enter__(self):
        self.path = tempfile.mkdtemp(*self.__args, **self.__kwargs)
        return self.path

    def __exit__(self, type, value, traceback):
        try:
            os.rmdir(self.path)
        except OSError, IOError:
            pass


class ChDir(object):
    def __init__(self, nwd):
        self.nwd = nwd

    def __enter__(self):
        self.orig_cwd = os.getcwd()
        os.chdir(self.nwd)

    def __exit__(self, type, value, traceback):
        os.chdir(self.orig_cwd)


def cycle_list_type(val):
    return sorted(set(int(v) for v in val.split(',')))


term_signal_names = ['SIGTERM', 'SIGHUP', 'SIGINT']
term_signals = [getattr(signal, name) for name in term_signal_names]


def main():
    parser = argparse.ArgumentParser(description='Runs an s3ql backup')
    parser.add_argument('storage_url',
                        help='s3ql storage URL')
    parser.add_argument('backup_dir',
                        help='The directory to be backed up.')
    parser.add_argument('-c', '--cycles', type=cycle_list_type,
                        default=[1, 7, 14, 31, 90, 180, 360],
                        help='Comma-separated list of cycles, in days, to '\
                             'pass on to s3ql-remove-backups')
    parser.add_argument('-F', '--no-fsck', action='store_false', default=True,
                        dest='fsck',
                        help='Do not perform initial fsck.')
    parser.add_argument('-I', '--no-interruptions', action='store_true',
                        default=False,
                        help='Ignore %s et al.' % ', '.join(term_signal_names))
    parser.add_argument('--maxtries', type=int, default=5,
                        help='Maximum amount of unmount tries.')
    parser.add_argument('--ssl', action='store_true', default=False,
                        help='Pass --ssl to mount.s3ql.')

    args = parser.parse_args()

    if args.no_interruptions:
        with SignalMask(signal.SIG_IGN, term_signals):
            return do_backup(args)
    else:
        return do_backup(args)


def do_backup(args):
# Recover cache if e.g. system was shut down while fs was mounted
    if args.fsck:
        # never interrupt fsck
        with SignalMask(signal.SIG_IGN, term_signals):
            subprocess.check_call(['fsck.s3ql', '--batch', args.storage_url])

# Create a temporary mountpoint and mount file system
    with TempDir(prefix='s3ql-backup-mnt') as mountpoint:
        mounted = False
        try:
            # do not disturb mounting/unmount
            with SignalMask(signal.SIG_IGN, term_signals):
                mount_args = ['mount.s3ql']
                if args.ssl:
                    mount_args.append('--ssl')
                mount_args.extend([args.storage_url, mountpoint])
                subprocess.check_call(mount_args)
                mounted = True

            # determine the most recent backup
            backups = sorted(x for x in os.listdir(mountpoint)\
                      if re.match(r'^[\d-]{10}_[\d:]{8}$', x))
            last_backup = backups[-1] if backups else None
            new_backup = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

            # add full paths, since we're not chdir'ing into the backup dir
            last_backup_path = os.path.join(mountpoint, last_backup)
            new_backup_path = os.path.join(mountpoint, new_backup)

            # duplicate the most recent backup unless this is the first backup
            if last_backup:
                print 'Copying %s to %s' % (last_backup, new_backup)

                # if s3qlcp fails, no problem, things still get cleaned up
                s3qlcp_args = ['s3qlcp', last_backup_path, new_backup_path]
                subprocess.check_call(s3qlcp_args)

                # make the last backup immutable
                # (in case the previous backup was interrupted prematurely)
                subprocess.check_call(['s3qllock', last_backup_path])

            # perform actual backup
            rsync_args = ['rsync', '-aHAXx', '--delete-during',
                          '--delete-excluded', '--partial', '-v',
                          '--exclude', '/.cache/', '--exclude', '/.s3ql/',
                          '--exclude', '/.thumbnails/', '--exclude', '/tmp/',
                          args.backup_dir,
                          os.path.normpath(new_backup_path) + os.sep]


            subprocess.check_call(rsync_args)

            # make the new backup immutable
            subprocess.check_call(['s3qllock', new_backup_path])

            # expire backups
            with ChDir(mountpoint):
                s3qleb_args = ['s3ql-expire_backups', '--use-s3qlrm']
                s3qleb_args.extend(str(n) for n in args.cycles)

                subprocess.check_call(s3qleb_args)

            print "Backup completed without errors, now unmounting..."

        finally:
            if mounted:
                # do not interrupt unmounting
                with SignalMask(signal.SIG_IGN, term_signals):
                    for i in xrange(args.maxtries):
                        rval = subprocess.call(['umount.s3ql', mountpoint])

                        if 4 == rval:
                            print 'Warning, mount point in use. Trying again '\
                                  'in one second...'
                            time.sleep(1)
                        else:
                            print 'Unmounted', mountpoint
                            break

                    if rval:
                        raise IOError('Unmounting failed.')


if __name__ == '__main__':
    # this ensures cleanup is run properly
    with SignalMask(on_signal_raise, term_signals):
        main()
