#!/usr/bin/env python

from optparse import OptionParser
import sys
import re
import os
import glob

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
    if not options:
        options = {}

    if options.ignore:
        ignored_names = (options.ignore.split(','))
    else:
        ignored_names = []

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

        if options.minutes:
            found_time = found_time[:-3]
        elif options.hours:
            found_time = found_time[:-6]

        time_key = build_date(date) + found_time
        build_user_list(user, time_key, user_lists)
        build_total_list(time_key, total_list)

    build_user_files(user_lists, output_dir)
    build_total_file(total_list, output_dir)


def build_date(date):
    month = date[:3]
    day = date[4:]
    return "2015"+months.get(month)+day + ", "


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

    parser = OptionParser()
    parser.add_option("-i", "--ignore", dest="ignore", help="user(s) to ignore. In quotations with NO SPACES", metavar="IGNORE")
    parser.add_option("-m", "--minutes", action="store_true", dest="minutes", help="data by minutes (default to seconds)", metavar="MINUTES")
    parser.add_option("-l", "--hours", dest="hours", action="store_true", help="data by hours (default to seconds)", metavar="HOURS")

    (options, args) = parser.parse_args()

    if len(sys.argv) < 2:
        print "Missing log file or log folder to parse. Exiting."
        exit()

    file_or_dir = sys.argv[1]

    if os.path.isdir(file_or_dir):
        for file_name in glob.glob(file_or_dir+'/*.log'):
            parse_file(file_name, str(file_name) + '_parser_output', options)
    else:
        parse_file(file_or_dir, './' + str(file_or_dir) + '_parser_output/', options)


if __name__ == "__main__":
    main()