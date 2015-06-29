#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime, date, timedelta
import argparse


total_aggregate_list = {}


def parse_file(filename, output_dir, options):
    if options.ignore:
        ignored_names = (options.ignore.split(','))
    else:
        ignored_names = []

    if options.year:
        year = str(options.year)
    else:
        year = str(date.today().year) + " "

    zeroes = options.zeroes

    user_lists = {}
    total_list = {}

    if options.minute_format:
        time_format = 60
    elif options.hour_format:
        time_format = 60 * 60
    else:
        time_format = 1

    f = open(filename, 'r')

    for line in f:
        line_text = str(line)

        try:
            started_line = re.search('started', line_text)
            user = re.search('puser=([a-zA-Z0-9_-]+)', line_text).group(1)
            date_time = re.search('[A-Z][a-z][a-z] [0-3][0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line_text).group(0)
        except:
            continue

        if user in ignored_names:
            continue

        date_with_year = str(year) + date_time

        date_key = format_date(date_with_year)

        if options.minute_format:
            date_key = date_key[:-3]
        elif options.hour_format:
            date_key = date_key[:-6]

        build_user_list(user, date_key, user_lists)
        build_total_list(date_key, total_list)

    build_user_files(user_lists, output_dir, zeroes, time_format)
    build_total_file(total_list, output_dir, zeroes, time_format)


def build_user_files(user_lists, directory='./', zeroes=None, time_format=1):
    if not os.path.exists(directory):
        os.makedirs(directory)

    for user in user_lists:
        if zeroes:
            user_lists[user] = add_zeroes(user_lists[user], time_format)

        #user_lists[user] = formatted(user_lists[user], time_format)

        f = open(str(directory) + '/' + str(user) + '.out', 'w+')
        for key in sorted(user_lists[user].keys()):
            f.write(str(key) + ", " + str(user_lists[user][key]) + "\n")
        f.close()


def format_date(date_string):
    year, month, day, time = date_string.split(' ')
    month_num = get_month_num(month)
    return year+month_num+day + ", " + time


# def formatted(list_in, format):
#     for key in list_in.keys():
#         if format == 60:
#             new_key = datetime.strftime(key, "%Y%m%d, %H:%M")
#         elif format == 3600:
#             new_key = datetime.strftime(key, "%Y%m%d, %H")
#         else:
#             new_key = datetime.strftime(key, "%Y%m%d, %H:%M:%S")
#         list_in[new_key] = list_in.pop(key)
#     return list_in


def build_total_file(total_list, directory='./', zeroes=None, time_format=1):
    if not os.path.exists(directory):
        os.makedirs(directory)
    if zeroes:
        total_list = add_zeroes(total_list, time_format)

    #total_list = formatted(total_list, time_format)

    build_total_aggregate_list(total_list)

    f = open(str(directory) + '/parsertotal.out', 'w+')
    for item in sorted(total_list):
        f.write(str(item) + ", " + str(total_list[item]) + "\n")
    f.close()


def build_total_aggregate_file(directory='./'):
    if not os.path.exists(directory):
        os.makedirs(directory)

    f = open(str(directory) + '/log_aggregate_total.out', 'w+')
    for item in sorted(total_aggregate_list):
        f.write(str(item) + ", " + str(total_aggregate_list[item]) + "\n")
    f.close()


def build_user_list(user, time_key, user_lists):
    if user not in user_lists:
        user_lists[user] = {}
    if time_key not in user_lists[user]:
        user_lists[user][time_key] = 1
    else:
        user_lists[user][time_key] += 1


def build_total_list(time_key, total_list):
    if time_key not in total_list:
        total_list[time_key] = 1
    else:
        total_list[time_key] += 1


def build_total_aggregate_list(list_in):
    global total_aggregate_list
    if total_aggregate_list is None:
        total_aggregate_list = list_in.copy
    else:
        total_aggregate_list.update(list_in)


def date_increment(date, time_format):
    date, time = date.split(',')
    time = time[1:]
    last_digit = time[len(time) - 1]
    print last_digit


def add_zeroes(time_list, time_format=1):
    sorted_keys = sorted(time_list.keys())
    first_element = sorted_keys[0]
    last_element = sorted_keys[-1]
    zero_list = {}

    while first_element < last_element:
        if first_element not in time_list:
            zero_list[first_element] = 0
        first_element = first_element + date_increment(first_element, time_format)
                        #timedelta(0, time_format)
    total = time_list.copy()
    total.update(zero_list)
    return total


def get_month_num(month_string):
    if month_string == "Jan":
        return "01"
    elif month_string == "Feb":
        return "02"
    elif month_string == "Mar":
        return "03"
    elif month_string == "Apr":
        return "04"
    elif month_string == "May":
        return "05"
    elif month_string == "Jun":
        return "06"
    elif month_string == "Jul":
        return "07"
    elif month_string == "Aug":
        return "08"
    elif month_string == "Sep":
        return "09"
    elif month_string == "Oct":
        return "10"
    elif month_string == "Nov":
        return "11"
    elif month_string == "Dec":
        return "12"
    else:
        return "none"


def main():
    parser = argparse.ArgumentParser(description="Count iRODS user connections")
    parser.add_argument(metavar='FILE_OR_DIR', dest='file_or_dir', help=".log file or folder containing .log files to parse.")
    parser.add_argument('--hours', dest='hour_format', action='store_const', const="hour", help="Output connections by hour (defaults to seconds)")
    parser.add_argument('-m', '--minutes', dest='minute_format', action='store_const', const="minute", help="Output connections by minute (defaults to seconds)")
    parser.add_argument('-y', '--year', dest='year', help="Year of log (defaults to current year")
    parser.add_argument('-i', '--ignore', dest='ignore', help="Users to ignore")
    parser.add_argument('-o', '--output', dest="output", help="Directory for output files (defaults to CWD)")
    parser.add_argument('-z', '--zeroes', action='store_const', dest='zeroes', const='zeroes', help="Output zeroes for non existent time")
    parser.add_argument('-t', '--total', action='store_const', dest='total', const='total', help="Generate a total aggregate of all log files")

    options = parser.parse_args()

    file_or_dir = options.file_or_dir
    output_folder = options.output or './'

    if os.path.isdir(file_or_dir):
        for file_name in glob.glob(file_or_dir+'/*.log'):
            parse_file(file_name, output_folder + str(file_name) + '_parser_output/', options)
    else:
        parse_file(file_or_dir, output_folder + str(file_or_dir) + '_parser_output/', options)

    if options.total:
        build_total_aggregate_file(output_folder)


if __name__ == "__main__":
    main()