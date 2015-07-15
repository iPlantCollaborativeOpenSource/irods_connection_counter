#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime, date, timedelta
import argparse
import time
from multiprocessing import Pool, Process


# #####################################################
# # Add zeroes at times where no connections occurred #
# #####################################################
# def add_zeroes(time_list, time_format=1):
#     # Need to sort keys to accurately get first and last connection times
#     sorted_keys = sorted(time_list.keys())
#     first_element = sorted_keys[0]
#     last_element = sorted_keys[-1]
#     zero_list = {}
#
#     # iterate through found times from first to last, maintain separate list of times with no connections
#     while first_element < last_element:
#         if first_element not in time_list:
#             zero_list[first_element] = 0
#         # increment time value up depending on format specified by user. Defaults to 1 second
#         first_element = first_element + timedelta(0, time_format)
#     total = time_list.copy()
#     total.update(zero_list)
#     return total
#
#
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
    user_lists = {}
    process_ids = {}
    old_connections = {}
    leftover_opened_count = 0
    #print options
    resolution = 1
    if options.minute_format:
        resolution = 60
    elif options.hour_format:
        resolution = 60 * 60
    ignored_users = options.ignore or []

    with open(file_name, 'r') as f:
        for line in f:
            if re.search('started', line):
                try:
                    pid = re.search('process ([0-9]+)', line).group(1)
                    user = re.search('puser=([a-zA-Z0-9_-]+)', line).group(1)
                    date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
                except Exception as e:
                    continue

                if user in ignored_users:
                        continue

                if resolution == 60:
                    date_time = date_time[:-3]
                elif resolution == 3600:
                    date_time = date_time[:-6]

                if pid not in process_ids or process_ids[pid]['closed']:
                    leftover_opened_count += 1
                    process_ids[pid] = {'user': user, 'opened': date_time, 'closed': None}

            elif re.search('exited', line):
                try:
                    pid = re.search('process ([0-9]+)', line).group(1)
                    date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
                except Exception as e:
                    continue

                if resolution == 60:
                    date_time = date_time[:-3]
                elif resolution == 3600:
                    date_time = date_time[:-6]

                if pid not in process_ids:
                    if date_time not in old_connections:
                        old_connections[date_time] = 1
                    else:
                        old_connections[date_time] += 1
                else:
                    leftover_opened_count -= 1
                    process_ids[pid]['closed'] = date_time
            else:
                continue
    f.close()

    formatted_old_connections = {}
    old_connection_count = 0

    print resolution

    date_format = "%Y %b %d %H:%M:%S"
    if resolution == 60:
        date_format = "%Y %b %d %H:%M"
    elif resolution == 3600:
        date_format = "%Y %b %d %H"

    for key in old_connections:
            opened_time = "2015 " + str(key)
            date_object = datetime.strptime(opened_time, date_format)

            time_stamp = int(time.mktime(date_object.timetuple()))
            formatted_old_connections[time_stamp] = old_connections[key]
            old_connection_count += old_connections[key]

    for pid in process_ids:
        username = process_ids[pid]['user']
        opened_time = None
        closed_time = None

        if process_ids[pid]['opened']:
            opened_time = "2015 " + process_ids[pid]['opened']
            date_object = datetime.strptime(opened_time, date_format)
            opened_time = int(time.mktime(date_object.timetuple()))

        if process_ids[pid]['closed']:
            closed_time = "2015 " + process_ids[pid]['closed']
            date_object = datetime.strptime(closed_time, date_format)
            closed_time = int(time.mktime(date_object.timetuple()))

        if username not in user_lists:
            user_lists[username] = {'opened': {}, 'closed': {}}

        if opened_time:
            if opened_time not in user_lists[username]['opened']:
                user_lists[username]['opened'][opened_time] = 1
            else:
                user_lists[username]['opened'][opened_time] += 1

        if closed_time:
            if closed_time not in user_lists[username]['closed']:
                user_lists[username]['closed'][closed_time] = 1
            else:
                user_lists[username]['closed'][closed_time] += 1

    for username in user_lists:
        user_connections[username] = {}
        sorted_open_connections = sorted(user_lists[username]['opened'])
        sorted_closed_connections = sorted(user_lists[username]['closed'])
        first = sorted_open_connections[0]
        last = sorted_open_connections[len(sorted_open_connections) - 1]
        try:
            first_closed = sorted_closed_connections[0]
            last_closed = sorted_closed_connections[len(sorted_closed_connections) - 1]
        except Exception as e:
            first_closed = first
            last_closed = last
        if first_closed < first:
            first = first_closed
        if last_closed > last:
            last = last_closed

        open_connections = 0
        while first < last:
            if first in user_lists[username]['opened']:
                open_connections += user_lists[username]['opened'][first]
            if first in user_lists[username]['closed']:
                open_connections -= user_lists[username]['closed'][first]
            user_connections[username][first] = open_connections
            first += resolution

    total_list = {}
    for key in user_connections:
        print key
        with open('hello/test_out_' + key, 'w+') as f:
            for connection_time in user_connections[key]:
                if connection_time not in total_list:
                    total_list[connection_time] = user_connections[key][connection_time]
                else:
                    total_list[connection_time] += user_connections[key][connection_time]
                #print connection_time
                time_to_write = format_date(connection_time)
                f.write(time_to_write + ", " + str(user_connections[key][connection_time]) + "\n")
        f.close()
    with open('all_total', 'w+') as f:
        for key in sorted(total_list):
            time_to_write = format_date(key)
            f.write(time_to_write + ", " + str(total_list[key]) + '\n')
    f.close()

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
