#!/usr/bin/env python

import re
import os
import glob
import datetime
import argparse

months = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12"
}


def parse_file(filename, output_dir, options):
    if options.ignore:
        ignored_names = (options.ignore.split(','))
    else:
        ignored_names = []

    if options.year:
        year = str(options.year)
    else:
        year = str(datetime.date.today().year)

    user_lists = {}
    total_list = {}

    f = open(filename, 'r')

    for line in f:
        line_text = str(line)

        try:
            started_line = re.search('started', line_text)
            user = re.search('puser=(\w+)', line_text).group(1)
            date = re.search('[A-Z][a-z][a-z] [0-9][0-9]*', line_text).group(0)
            found_time = re.search('[0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line_text).group(0)
        except:
            continue

        if user in ignored_names:
            continue

        if options.minute_format:
            found_time = found_time[:-3]
        elif options.hour_format:
            found_time = found_time[:-6]

        time_key = build_date(date, year) + found_time
        build_user_list(user, time_key, user_lists)
        build_total_list(time_key, total_list)

    build_user_files(user_lists, output_dir)
    build_total_file(total_list, output_dir)


def build_date(date, year):
    month = date[:3]
    day = date[4:]
    return year+months.get(month)+day + ", "


def build_user_files(user_lists, directory='./'):
    if not os.path.exists(directory):
        os.makedirs(directory)
    for user in user_lists:
        f = open(str(directory) + '/' + str(user) + '.out', 'w+')
        for key in sorted(user_lists[user].keys()):
            f.write(str(key) + ", " + str(user_lists[user][key]) + "\n")
        f.close()


def build_total_file(total_list, directory='./'):
    if not os.path.exists(directory):
        os.makedirs(directory)
    f = open(str(directory) + '/parsertotal.out', 'w+')
    for item in sorted(total_list):
        f.write(str(item) + ", " + str(total_list[item]) + "\n")
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


def main():
    parser = argparse.ArgumentParser(description="Count iRODS user connections")
    parser.add_argument(metavar='FILE_OR_DIR', dest='file_or_dir', help=".log file or folder containing .log files to parse.")
    parser.add_argument('--hours', dest='hour_format', action='store_const', const="hour", help="Output connections by hour (defaults to seconds)")
    parser.add_argument('-m', '--minutes', dest='minute_format', action='store_const', const="minute", help="Output connections by minute (defaults to seconds)")
    parser.add_argument('-y', '--year', dest='year', help="Year of log (defaults to current year")
    parser.add_argument('-i', '--ignore', dest='ignore', help="Users to ignore")
    parser.add_argument('-o', '--output', dest="output", help="Directory for output files (defaults to CWD)")

    options = parser.parse_args()

    file_or_dir = options.file_or_dir
    output_folder = options.output or './'

    if os.path.isdir(file_or_dir):
        for file_name in glob.glob(file_or_dir+'/*.log'):
            parse_file(file_name, output_folder + str(file_name) + '_parser_output/', options)
    else:
        parse_file(file_or_dir, output_folder + str(file_or_dir) + '_parser_output/', options)


if __name__ == "__main__":
    main()