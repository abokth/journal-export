#!/usr/bin/python

#   Copyright 2015 Alexander Boström, Kungliga Tekniska högskolan
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# For general info about the format, see:
#  http://www.freedesktop.org/wiki/Software/systemd/json
# For the meaning of the fields, see:
#  http://www.freedesktop.org/software/systemd/man/systemd.journal-fields.html
#
# Extra fields are added, but they should generally not conflict with the fields set by journald.
#

import os
import sys
import subprocess
import socket
from datetime import datetime
from tzlocal import get_localzone
import json

empty = {}

syslog_facilities = { u"0": u"kern", 0: u"kern", u"1": u"ftp", 1: u"ftp", u"2": u"mail", 2: u"mail", u"3": u"daemon", 3: u"daemon", u"4": u"auth", 4: u"auth", u"5": u"syslog", 5: u"syslog", u"6": u"lpr", 6: u"lpr", u"7": u"news", 7: u"news", u"8": u"uucp", 8: u"uucp", u"9": u"cron", 9: u"cron", u"10": u"authpriv", 10: u"authpriv", u"11": u"user", 11: u"user", u"16": u"local0", 16: u"local0", u"17": u"local1", 17: u"local1", u"18": u"local2", 18: u"local2", u"19": u"local3", 19: u"local3", u"20": u"local4", 20: u"local4", u"21": u"local5", 21: u"local5", u"22": u"local6", 22: u"local6", u"23": u"local7", 23: u"local7" }

syslog_severities = { u"0": u"emerg", 0: u"emerg", u"1": u"alert", 1: u"alert", u"2": u"crit", 2: u"crit", u"3": u"err", 3: u"err", u"4": u"warn", 4: u"warn", u"5": u"notice", 5: u"notice", u"6": u"info", 6: u"info", u"7": u"debug", 7: u"debug" }

reserved_keys = [u'_id', u'_index', u'_type']

tz = get_localzone()

cursor = None

# This file is used to keep track of the last exported entry.
cursor_currentfile = "/var/run/journal2export.cursor"
cursor_tmpfile = "%s.new.%d" % (cursor_currentfile, os.getpid())

try:
    with open(cursor_currentfile, "r") as cursor_file:
        cursor_data = json.load(cursor_file)
        cursor = cursor_data[u'cursor']
except:
    pass

journalctl_commandline = ["/usr/bin/journalctl", "--output=json", "--follow"]
if cursor:
    journalctl_commandline.append("--after-cursor=%s" % cursor)

with open('/dev/null', 'r') as devnull:
    p = subprocess.Popen(journalctl_commandline, bufsize=1, stdin=devnull, stdout=subprocess.PIPE, close_fds=True)

    for line in p.stdout:
        data = empty
        # JSON is always UTF-8.
        try:
            unicode_line = line.decode('utf8')
            try:
                data = json.loads(unicode_line)
            except ValueError:
                # Not valid JSON. Treat as unicode string an encode as JSON.
                data = {u'invalid_json': unicode_line.rstrip()}
        except UnicodeDecodeError:
            # Not valid UTF-8. Treat as byte array and encode as JSON.
            data = {u'invalid_json': data.rstrip()}

        # Make a shallow copy.
        data = data.copy()

        def rename(key):
            orig_key = u'orig_%s' % key
            if data.has_key(orig_key):
                rename(orig_key)
            data[orig_key] = data.pop(key)

        # These are set on the receiver side.
        for reserved_key in reserved_keys:
            if data.has_key(reserved_key):
                rename_key(reserved_key)

        # Make sure 'host' is set.
        if data.has_key(u'host'):
            if data.has_key(u'_HOSTNAME'):
                if data[u'host'] != data[u'_HOSTNAME']:
                    rename_key(u'host')
                    data[u'host'] = socket.gethostname()
        else:
            if data.has_key(u'_HOSTNAME'):
                #data[u'host'] = data.pop(u'_HOSTNAME')
                data[u'host'] = data[u'_HOSTNAME']
            else:
                data[u'host'] = socket.gethostname()

        # Make sure '@message' is set.
        if not data.has_key(u'@message'):
            if data.has_key(u'MESSAGE'):
                #data[u'@message'] = data.pop(u'MESSAGE')
                data[u'@message'] = data[u'MESSAGE']

        # Make sure '@timestamp' is set.
        if data.has_key(u'@timestamp'):
            rename_key(u'@timestamp')
        timestamp_string = None
        if data.has_key(u'_SOURCE_REALTIME_TIMESTAMP'):
            timestamp_string = data[u'_SOURCE_REALTIME_TIMESTAMP']
        elif data.has_key(u'__REALTIME_TIMESTAMP'):
            timestamp_string = data[u'__REALTIME_TIMESTAMP']
        dt = None
        if timestamp_string:
            try:
                timestamp = int(timestamp_string)
                dt = datetime.fromtimestamp(timestamp/1000.0, tz=tz).isoformat()
            except ValueError:
                pass
        if not dt:
            dt = datetime.now(tz=tz)
        isodt = dt.isoformat()
        data[u'@timestamp'] = isodt

        # Set traditional syslog fields.
        if data.has_key(u'PRIORITY'):
            severity = data[u'PRIORITY']
            if syslog_severities.has_key(severity):
                severity = syslog_severities[severity]
            if data.has_key(u'severity'):
                if data[u'severity'] != severity:
                    rename_key(u'severity')
                    data[u'severity'] = severity
            else:
                data[u'severity'] = severity

        if data.has_key(u'SYSLOG_FACILITY'):
            facility = data[u'SYSLOG_FACILITY']
            if syslog_facilities.has_key(facility):
                facility = syslog_facilities[facility]
            if data.has_key(u'facility'):
                if data[u'facility'] != facility:
                    rename_key(u'facility')
                    data[u'facility'] = facility
            else:
                data[u'facility'] = facility

        json_data = json.dumps(data, sort_keys=True, indent=4)
        print json_data

        if data.has_key(u'__CURSOR'):
            cursor = data[u'__CURSOR']

            try:
                with open(cursor_tmpfile, "w") as cursor_file:
                    cursor_data = {u'cursor': cursor}
                    json.dump(cursor_data, cursor_file)
                os.rename(cursor_tmpfile, cursor_currentfile)
            except:
                pass
