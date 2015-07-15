#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime, date, timedelta
import argparse
import time
from multiprocessing import Pool, Process


# months = {
#     "Jan": "01",
#     "Feb": "02",
#     "Mar": "03",
#     "Apr": "04",
#     "May": "05",
#     "Jun": "06",
#     "Jul": "07",
#     "Aug": "08",
#     "Sep": "09",
#     "Oct": "10",
#     "Nov": "11",
#     "Dec": "12"
# }


# def parse_file(filename, output_dir, options):
#     if options.ignore:
#         ignored_names = (options.ignore.split(','))
#     else:
#         ignored_names = []
#
#     if options.year:
#         year = str(options.year)
#     else:
#         year = str(date.today().year) + " "
#
#     user_lists = {}
#
#     f = open(filename, 'r')
#
#     for line in f:
#         line_text = str(line)
#
#         try:
#             started_line = re.search('started', line_text)
#             user = re.search('puser=([a-zA-Z0-9_-]+)', line_text).group(1)
#             date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line_text).group(0)
#         except Exception as e:
#             continue
#
#         if user in ignored_names:
#             continue
#
#         date_with_year = str(year) + date_time
#
#         date_object = datetime.strptime(date_with_year, "%Y %b %d %H:%M:%S")
#
#         if options.minute_format:
#             date_object = date_object.replace(second=0, microsecond=0)
#         elif options.hour_format:
#             date_object = date_object.replace(minute=0, second=0, microsecond=0)
#
#         build_user_list(user, date_object, user_lists, output_dir)
#
#     build_user_files(user_lists, output_dir, options)
#     build_total_file(user_lists, output_dir, options)
#
#
# def parse_output_file(filename, aggregate_list):
#     f = open(filename, 'r')
#
#     for line in f:
#         line_text = str(line)
#         date_value, time_value, count = line_text.split(',')
#         key = date_value + time_value
#         count = count.lstrip()
#         if key not in aggregate_list:
#             aggregate_list[key] = count
#         else:
#             aggregate_list[key] += count
#
#     f.close()
#
#
# def do_stuff(current_user_list, directory, user, time_format, zeroes):
#     print "user " + user
#     f = open(directory + user + '.parserout.tmp', 'r')
#     for date_and_time in f:
#         date_and_time = date_and_time.rstrip()
#         date_and_time = datetime.strptime(date_and_time, "%Y-%m-%d %H:%M:%S")
#         if date_and_time not in current_user_list:
#             current_user_list[date_and_time] = 1
#         else:
#             current_user_list[date_and_time] += 1
#     f.close()
#     os.remove(directory + user + '.parserout.tmp')
#
#     if zeroes:
#         current_user_list = add_zeroes(current_user_list, time_format)
#
#     current_user_list = formatted(current_user_list, time_format)
#
#     f = open(directory + user + '.parserout', 'a+')
#     for item in sorted(current_user_list):
#         f.write(str(item.rstrip()) + ', ' + str(current_user_list[item]) + '\n')
#     f.close()
#
#
# def do_stuff_zero(user_lists, directory, time_format, zeroes):
#     p = Pool(4)
#     for user in user_lists:
#         current_user_list = {}
#         p.map(do_stuff, {current_user_list, directory, user, time_format, zeroes})
#         # p = Process(target=do_stuff, args=(current_user_list, directory, user, time_format, zeroes, count))
#         # p.start()
#
#
# def build_user_files(user_lists, directory='./', options=None):
#     if options.minute_format:
#         time_format = 60
#     elif options.hour_format:
#         time_format = 60 * 60
#     else:
#         time_format = 1
#     if not os.path.exists(directory):
#         os.makedirs(directory)
#
#     do_stuff_zero(user_lists, directory, time_format, options.zeroes)
#     # q = Process(target=do_stuff_zero, args=(user_lists, directory, time_format, options.zeroes))
#     # q.start()
#     # q.join()
#     # for user in user_lists:
#     #     current_user_list = {}
#     #     p = Process(target=do_stuff, args=(current_user_list, directory, user, time_format, options.zeroes))
#     #     p.start()
#         #p.join()
#         #do_stuff(directory, user)
#
#         # current_user_list = {}
#         # f = open(directory + user + '.parserout.tmp', 'r')
#         # for date_and_time in f:
#         #     date_and_time = date_and_time.rstrip()
#         #     date_and_time = datetime.strptime(date_and_time, "%Y-%m-%d %H:%M:%S")
#         #     if date_and_time not in current_user_list:
#         #         current_user_list[date_and_time] = 1
#         #     else:
#         #         current_user_list[date_and_time] += 1
#         # f.close()
#
#         # os.remove(directory + user + '.parserout.tmp')
#         #
#         # if options.zeroes:
#         #     current_user_list = add_zeroes(current_user_list, time_format)
#         #
#         # current_user_list = formatted(current_user_list, time_format)
#         #
#         # f = open(directory + user + '.parserout', 'a+')
#         # for item in sorted(current_user_list):
#         #     f.write(str(item.rstrip()) + ', ' + str(current_user_list[item]) + '\n')
#         # f.close()
#
#
# def formatted(list_in, format_type):
#     for key in list_in.keys():
#         if format_type == 60:
#             new_key = datetime.strftime(key, "%Y%m%d, %H:%M")
#         elif format_type == 3600:
#             new_key = datetime.strftime(key, "%Y%m%d, %H")
#         else:
#             new_key = datetime.strftime(key, "%Y%m%d, %H:%M:%S")
#         list_in[new_key] = list_in.pop(key)
#     return list_in
#
#
# def build_total_file(user_lists, directory='./', options=None):
#     if options.minute_format:
#         time_format = 60
#     elif options.hour_format:
#         time_format = 60 * 60
#     else:
#         time_format = 1
#     total_list = {}
#     for user in user_lists:
#         f = open(directory + user + '.parserout', 'r')
#         for line in f:
#             date, time, count = line.split(',')
#             time = time.lstrip()
#             count = int(count.lstrip().rstrip())
#             date_time_key = date + time
#             if time_format == 60:
#                 date_and_time = datetime.strptime(date_time_key, "%Y%m%d%H:%M")
#             elif time_format == 3600:
#                 date_and_time = datetime.strptime(date_time_key, "%Y%m%d%H")
#             else:
#                 date_and_time = datetime.strptime(date_time_key, "%Y%m%d%H:%M:%S")
#
#             if date_and_time not in total_list:
#                 total_list[date_and_time] = count
#             else:
#                 total_list[date_and_time] += count
#         f.close()
#
#     if options.zeroes:
#         total_list = add_zeroes(total_list, time_format)
#
#     total_list = formatted(total_list, time_format)
#
#     f = open(directory + 'all_users.parserout', 'w+')
#     for key in sorted(total_list):
#         f.write(str(key) + ', ' + str(total_list[key]) + '\n')
#     f.close()
#
#
# def build_total_aggregate_file(directory='.'):
#     if not os.path.exists(directory):
#         os.makedirs(directory)
#     aggregate_list = {}
#     for f in glob.glob(directory + '/**/*_parser_output/all_users.parserout'):
#         parse_output_file(f, aggregate_list)
#     f = open(directory + '/aggregate_all_logs.parserout', 'w+')
#     for item in sorted(aggregate_list):
#        date, time = item.split(' ')
#        f.write(str(date) + ", " + time + ", " +str(aggregate_list[item]))
#     f.close()
#
#
# ####################################################################################
# # Build up a list of users found in the file and write connection instance to file #
# ####################################################################################
# def build_user_list(user, time_key, user_lists, output_dir='./'):
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
#
#     # write found times to a temp file to be parsed later
#     f = open(output_dir + user + '.parserout.tmp', 'a+')
#     f.write(str(time_key) + '\n')
#     f.close()
#
#     # add user to a dictionary instead of array for constant retrieval
#     if user not in user_lists:
#         user_lists[user] = 0
#
#
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


# def connection_opened(line):
#     try:
#         re.search('started', line).group(0)
#     except Exception as e:
#         return False
#
#     return True
#
#
# def connection_closed(line):
#     try:
#         re.search('exited', line).group(0)
#     except Exception as e:
#         return False
#     return True


def parse_file(file_name, output_folder, options):
    users = {}
    user_lists = {}
    process_ids = {}
    old_connections = {}
    left_overs = {}
    leftover_opened_count = 0
    with open(file_name, 'r') as f:
        for line in f:
            if re.search('started', line):
                try:
                    pid = re.search('process ([0-9]+)', line).group(1)
                    user = re.search('puser=([a-zA-Z0-9_-]+)', line).group(1)
                    date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
                except Exception as e:
                    continue

                #date_with_year = "2015 " + date_time
                #date_object = datetime.strptime(date_with_year, "%Y %b %d %H:%M:%S")
                #timestamp = int(time.mktime(date_object.timetuple()))
                if pid not in process_ids or process_ids[pid]['closed']:
                    leftover_opened_count += 1
                    left_overs[pid] = date_time
                    process_ids[pid] = {'user': user, 'opened': date_time, 'closed': None}
            elif re.search('exited', line):
                try:
                    pid = re.search('process ([0-9]+)', line).group(1)
                    date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
                except Exception as e:
                    continue
                #date_with_year = "2015 " + date_time
                #date_object = datetime.strptime(date_with_year, "%Y %b %d %H:%M:%S")
                #timestamp = int(time.mktime(date_object.timetuple()))
                if pid not in process_ids:
                    if date_time not in old_connections:
                        old_connections[date_time] = 1
                    else:
                        old_connections[date_time] += 1
                else:
                    leftover_opened_count -= 1
                    del left_overs[pid]
                    process_ids[pid]['closed'] = date_time
            else:
                continue
    f.close()

    formatted_old_connections = {}
    for key in old_connections:
            opened_time = "2015 " + str(key)
            date_object = datetime.strptime(opened_time, "%Y %b %d %H:%M:%S")
            time_stamp = int(time.mktime(date_object.timetuple()))
            formatted_old_connections[time_stamp] = old_connections[key]
    #         if connection_opened(line):
    #             try:
    #                 re.search('started', line).group(0)
    #                 pid = re.search('process ([0-9]+)', line).group(1)
    #                 user = re.search('puser=([a-zA-Z0-9_-]+)', line).group(1)
    #                 date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
    #             except Exception as e:
    #                 continue
    #             date_with_year = "2015 " + date_time
    #             date_object = datetime.strptime(date_with_year, "%Y %b %d %H:%M:%S")
    #             timestamp = int(time.mktime(date_object.timetuple()))
    #             if pid not in process_ids or process_ids[pid]['closed']:
    #                 leftover_opened_count += 1
    #                 process_ids[pid] = {'user': user, 'opened': timestamp, 'closed': None}
    #
    #         elif connection_closed(line):
    #             try:
    #                 pid = re.search('process ([0-9]+)', line).group(1)
    #                 date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line).group(0)
    #             except Exception as e:
    #                 continue
    #             date_with_year = "2015 " + date_time
    #             date_object = datetime.strptime(date_with_year, "%Y %b %d %H:%M:%S")
    #             timestamp = int(time.mktime(date_object.timetuple()))
    #             if pid not in process_ids:
    #                 old_connections.append(timestamp)
    #
    #             else:
    #                 leftover_opened_count -= 1
    #                 process_ids[pid]['closed'] = timestamp

    for pid in process_ids:
        username = process_ids[pid]['user']
        opened_time = None
        closed_time = None

        if process_ids[pid]['opened']:
            opened_time = "2015 " + process_ids[pid]['opened']
            date_object = datetime.strptime(opened_time, "%Y %b %d %H:%M:%S")
            opened_time = int(time.mktime(date_object.timetuple()))

        if process_ids[pid]['closed']:
            closed_time = "2015 " + process_ids[pid]['closed']
            date_object = datetime.strptime(closed_time, "%Y %b %d %H:%M:%S")
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
        with open('hello/test_out_' + username, 'w+') as f:
            while first <= last:
                if first in user_lists[username]['opened']:
                    open_connections += user_lists[username]['opened'][first]
                if first in user_lists[username]['closed']:
                    open_connections -= user_lists[username]['closed'][first]
                f.write(str(first) + '\t')
                f.write(str(open_connections) + '\n')
                first += 1
        f.close()

    total_list = {}
    for file_name in glob.glob('hello/test_out*'):
        with open(file_name, 'r') as f:
            for line in f:
                closed_previous_connections = 0
                time_stamp, count = line.split('\t')
                time_stamp = int(time_stamp)
                count = int(count.rstrip())
                #print type(time_stamp)
                if time_stamp in formatted_old_connections:
                    closed_previous_connections = formatted_old_connections[time_stamp]

                if time_stamp not in total_list:
                    #print "adding " + str(count) + " to " + str(time_stamp)
                    total_list[time_stamp] = count + len(formatted_old_connections.values())
                else:
                    #print "adding " + str(count) + " to " + str(time_stamp) + " which was " + str(total_list[time_stamp])
                    total_list[time_stamp] += count


                total_list[time_stamp] -= closed_previous_connections



        f.close()
        with open('all_total', 'w+') as f:
            for time_stamp in sorted(total_list):
                f.write(str(time_stamp) + '\t' + str(total_list[time_stamp]) + '\n')
        f.close()
    print "old connections"
    print formatted_old_connections
        #print total_list
    print leftover_opened_count
    print "leftovers"
    print left_overs

        #print sorted_open_connections
        #sorted_connections = sorted(user_lists[user])
        #first = sorted_connections[0]
        #last = sorted_connections[len(sorted_connections) - 1]
        #with open('test.out', 'w+') as f:
        #    f.write()
        #while first < last:

        #print last
        #if username not in users:
        #    users[username] = {'connections': [{'opened': process_ids[pid]['opened'], 'closed': process_ids[pid]['closed']}]}
        #else:
        #    users[username]['connections'].append({'opened': process_ids[pid]['opened'], 'closed': process_ids[pid]['closed']})
            #users[username]['connections']['closed'] = process_ids[pid]['closed']
    # #print old_connections
    #print users
    #for username in users:
        #print sorted(users[username]['connections'])
        #user_lists[username] = users[username]['connections']
    #print user_lists
        #user_connections = sorted(users[username][0]['opened'])
        #print "sorted user connections"
        #print user_connections
        #print "user " + user + str(users[username])
        #print user + str(users[user])
    #print "opened count was " + str(leftover_opened_count)
    #print "closed " + str(len(old_connections)) + " leftover connections"
    #    print "user: " + str(process_ids[pid]['user']) + " opened: " + str(pid) + " " + str(process_ids[pid]['opened']) + " closed: " + str(process_ids[pid]['closed'])
    #print "connections: " + str(process_ids)


def main():
    parser = argparse.ArgumentParser(description="Count iRODS user connections")
    parser.add_argument(metavar='FILE_OR_DIR', dest='file_or_dir', help=".log file or folder containing .log files to parse.")
    parser.add_argument('--hours', dest='hour_format', action='store_const', const="hour", help="Output connections by hour (defaults to seconds)")
    parser.add_argument('-m', '--minutes', dest='minute_format', action='store_const', const="minute", help="Output connections by minute (defaults to seconds)")
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
