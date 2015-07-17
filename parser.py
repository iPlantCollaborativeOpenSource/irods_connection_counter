#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime, date, timedelta
import argparse
import copy
import time
from multiprocessing import Pool, Process


####################################
# Remove all previous output files #
####################################
def remove_previous_output(directory):
    for output_file in glob.glob(directory + '/**/*.parserout'):
        os.remove(output_file)


def format_date(unix_timestamp):
    date_object = time.localtime(unix_timestamp)
    return time.strftime("%Y%m%d, %H:%M:%S", date_object)


def parse_file(file_name, output_folder, options):
    user_connections = {}
    total_connections_for_file = {}
    old_connections = []
    user_lists = {}
    process_ids = {}
    resolution = 1

    date_format = "%Y %b %d %H:%M:%S"
    if options.minute_format:
        resolution = 60
        date_format = "%Y %b %d %H:%M"
    elif options.hour_format:
        resolution = 60 * 60
        date_format = "%Y %b %d %H"

    ignored_users = options.ignore or []

    with open(file_name, 'r') as f:
        leftovers = 0
        greatest_time = 0
        for line in f:
            # Check if line contains an open connection
            if re.search('started', line):
                try:
                    pid = re.search('process ([0-9]+)', line).group(1)
                    user = re.search('puser=([a-zA-Z0-9_-]+)', line).group(1)
                    date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
                except Exception as e:
                    continue

                if user in ignored_users:
                    process_ids[pid] = user
                    continue

                # Ignore seconds
                if resolution == 60:
                    date_time = date_time[:-3]
                # Ignore minutes and seconds
                elif resolution == 3600:
                    date_time = date_time[:-6]

                leftovers += 1

                opened_time = "2015 " + date_time
                date_object = datetime.strptime(opened_time, date_format)
                time_stamp = int(time.mktime(date_object.timetuple()))

                if time_stamp > greatest_time:
                    greatest_time = time_stamp

                if user not in user_connections:
                    user_connections[user] = {}
                if time_stamp not in user_connections[user]:
                    user_connections[user][time_stamp] = 0
                user_connections[user][time_stamp] += 1
                process_ids[pid] = user

            # Check if line contains a closed connection
            elif re.search('exited', line):
                try:
                    pid = re.search('process ([0-9]+)', line).group(1)
                    date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
                except Exception as e:
                    continue

                # Ignore seconds
                if resolution == 60:
                    date_time = date_time[:-3]

                # Ignore minutes and seconds
                elif resolution == 3600:
                    date_time = date_time[:-6]

                closed_time = "2015 " + date_time
                date_object = datetime.strptime(closed_time, date_format)
                time_stamp = int(time.mktime(date_object.timetuple()))

                if time_stamp > greatest_time:
                    greatest_time = time_stamp

                if pid in process_ids:
                    user = process_ids[pid]
                    if user in ignored_users:
                        del process_ids[pid]
                        continue
                    if user not in user_connections:
                        user_connections[user] = {}
                    if time_stamp not in user_connections[user]:
                        user_connections[user][time_stamp] = 0
                    user_connections[user][time_stamp] -= 1
                    leftovers -= 1
                    del process_ids[pid]
                else:
                    old_connections.append(time_stamp)
            else:
                continue
    f.close()

    del process_ids

    # Generate counts for each user
    for username in user_connections:
        count = 0
        user_lists[username] = {}
        for time_stamp in sorted(user_connections[username]):
            count += user_connections[username][time_stamp]
            user_lists[username][time_stamp] = count

    del user_connections

    # Pad out times where connections were sustained
    for username in user_lists:
        last = greatest_time
        for thing in sorted(user_lists[username]):
            while thing+1 not in user_lists[username] and thing + 1 <= last:
                user_lists[username][thing+1] = user_lists[username][thing]
                thing += 1

    for username in user_lists:
        old_connections_copy = copy.copy(old_connections)
        with open(output_folder + username + '.out', 'w+') as f:
            for time_stamp in sorted(user_lists[username]):

                if len(old_connections_copy) > 0 and time_stamp >= old_connections_copy[0]:
                    old_connections_copy.pop(0)
                if time_stamp not in total_connections_for_file:
                    total_connections_for_file[time_stamp] = len(old_connections_copy)

                total_connections_for_file[time_stamp] += user_lists[username][time_stamp]
                f.write(format_date(time_stamp) + ", " + str(user_lists[username][time_stamp]) + '\n')

                del(user_lists[username][time_stamp])

    #print "total"
    #for timestamp in sorted(total_connections_for_file):
    #   print timestamp
    #   print total_connections_for_file[timestamp]
    #print "leftovers: " + str(leftovers)


def main():
    parser = argparse.ArgumentParser(description="Count iRODS user connections")
    parser.add_argument(metavar='FILE_OR_DIR', dest='file_or_dir', help=".log file or folder containing .log files to parse.")
    parser.add_argument('--hours', dest='hour_format', action='store_const', const="hour", help="Output connections by hour (defaults to seconds)")
    parser.add_argument('-m', '--minutes', dest='minute_format', action='store_const', const="minutes", help="Output connections by minute (defaults to seconds)")
    parser.add_argument('-y', '--year', dest='year', help="Year of log (defaults to current year")
    parser.add_argument('-i', '--ignore', dest='ignore', help="Users to ignore")
    parser.add_argument('-o', '--output', dest="output", help="Directory for output files (defaults to CWD)")
    parser.add_argument('-z', '--zeroes', action='store_const', dest='zeroes', const='zeroes', help="Output zeroes for time with no connections")
    parser.add_argument('-t', '--total', action='store_const', dest='total', const='total', help="Generate a total aggregate of all log files")

    options = parser.parse_args()

    file_or_dir = options.file_or_dir
    if not options.output:
        output_folder = './'
    else:
        output_folder = options.output+'/'

    remove_previous_output(output_folder)

    if os.path.isdir(file_or_dir):
        for file_name in glob.glob(file_or_dir+'/*.log'):
            parse_file(file_name, output_folder + str(file_name) + '_parser_output/', options)
    else:
        parse_file(file_or_dir, output_folder + str(file_or_dir) + '_parser_output/', options)
    #
    # if options.total:
    #     build_total_aggregate_file(output_folder)


if __name__ == "__main__":
    main()
