#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime
import argparse
import copy
import gc
import time


####################################
# Remove all previous output files #
####################################
def remove_previous_output(directory):
    for output_file in glob.glob(directory + '/**/*.parserout'):
        os.remove(output_file)


# TODO: come up with an efficient and accurate way to combine all logs
# def build_total_aggregate_file(output_folder):
#     total_aggregate = {}
#     total_count = 0
#     greatest = 0
#     for filename in glob.glob(output_folder + '/**/*/all_users.out'):
#         with open(filename, 'r') as f:
#             for line in f:
#                 date, timestamp, count = line.split(',')
#                 timestamp = timestamp.lstrip()
#                 count = int(count.strip())
#                 date_object = datetime.strptime(str(date)+str(timestamp), "%Y%m%d%H:%M:%S")
#                 time_stamp = int(time.mktime(date_object.timetuple()))
#                 #total_count += count
#                 if time_stamp > greatest
#                 if time_stamp not in total_aggregate:
#                     total_aggregate[time_stamp] = 0
#                 total_aggregate[time_stamp] += count
#         f.close()
#
#     with open(output_folder+'all_logs.out', 'w+') as f:
#         for timestamp in sorted(total_aggregate):
#             f.write(str(timestamp) + ", " + str(total_aggregate[timestamp]) + "\n")
#         f.close()


def format_date(unix_timestamp):
    date_object = time.localtime(unix_timestamp)
    return time.strftime("%Y%m%d, %H:%M:%S", date_object)


def parse_file(file_name, output_folder, options):
    user_connections = {}
    total_connections_for_file = {}
    old_connections = []
    user_counts = {}
    process_ids = {}
    resolution = 1
    year = options.year or "2015"

    date_format = "%Y %b %d %H:%M:%S"
    if options.minute_format:
        resolution = 60
        date_format = "%Y %b %d %H:%M"
    elif options.hour_format:
        resolution = 60 * 60
        date_format = "%Y %b %d %H"

    ignored_users = options.ignore or []

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

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

                opened_time = year + " " + date_time
                date_object = datetime.strptime(opened_time, date_format)
                time_stamp = int(time.mktime(date_object.timetuple()))

                if time_stamp > greatest_time:
                    greatest_time = time_stamp

                if user not in user_counts:
                    user_counts[user] = 0
                user_counts[user] += 1

                if user not in user_connections:
                    user_connections[user] = {}

                user_connections[user][time_stamp] = int(user_counts[user])
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

                closed_time = year + " " + date_time
                date_object = datetime.strptime(closed_time, date_format)
                time_stamp = int(time.mktime(date_object.timetuple()))

                if time_stamp > greatest_time:
                    greatest_time = time_stamp

                if pid in process_ids:
                    user = process_ids[pid]
                    if user in ignored_users:
                        del process_ids[pid]
                        continue
                    user_counts[user] -= 1
                    user_connections[user][time_stamp] = int(user_counts[user])
                    leftovers -= 1
                    del process_ids[pid]
                else:
                    old_connections.append(time_stamp)
            else:
                continue
    f.close()

    process_ids.clear()
    gc.collect()

    user_names = copy.copy(user_connections.keys())

    # Pad out times where connections were sustained
    for username in user_names:
        old_connections_copy = copy.copy(old_connections)
        with open(output_folder + username + '.out', 'w+') as f:
            for timestamp in sorted(user_connections[username]):
                while timestamp + resolution not in user_connections[username] and timestamp + resolution <= greatest_time:
                    user_connections[username][timestamp+resolution] = user_connections[username][timestamp]
                    timestamp += resolution
            for timestamp in sorted(user_connections[username]):
                if len(old_connections_copy) > 0 and old_connections_copy[0] <= timestamp:
                    old_connections_copy.pop()
                if timestamp not in total_connections_for_file:
                    total_connections_for_file[timestamp] = len(old_connections_copy)
                total_connections_for_file[timestamp] += user_connections[username][timestamp]
                f.write(format_date(timestamp) + ", " + str(user_connections[username][timestamp]) + "\n")
            del user_connections[username]
            gc.collect()

    # write all connections to file
    with open(output_folder + 'all_users.out', 'w+') as f:
        for timestamp in sorted(total_connections_for_file):
            f.write(format_date(timestamp) + ", " + str(total_connections_for_file[timestamp]) + "\n")
    total_connections_for_file.clear()
    gc.collect()
    f.close()


def main():
    parser = argparse.ArgumentParser(description="Count iRODS user connections")
    parser.add_argument(metavar='FILE_OR_DIR', dest='file_or_dir', help=".log file or folder containing .log files to parse.")
    parser.add_argument('--hours', dest='hour_format', action='store_const', const="hour", help="Output connections by hour (defaults to seconds)")
    parser.add_argument('-m', '--minutes', dest='minute_format', action='store_const', const="minutes", help="Output connections by minute (defaults to seconds)")
    parser.add_argument('-y', '--year', dest='year', help="Year of log (defaults to current year")
    parser.add_argument('-i', '--ignore', dest='ignore', help="Users to ignore")
    parser.add_argument('-o', '--output', dest="output", help="Directory for output files (defaults to CWD)")
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

    #if options.total:
    #    build_total_aggregate_file(output_folder)


if __name__ == "__main__":
    main()
