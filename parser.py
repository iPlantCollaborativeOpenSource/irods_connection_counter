#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime, date, timedelta
import argparse
from multiprocessing import Pool


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
        year = str(date.today().year) + " "

    user_lists = {}

    f = open(filename, 'r')

    for line in f:
        line_text = str(line)

        try:
            started_line = re.search('started', line_text)
            user = re.search('puser=([a-zA-Z0-9_-]+)', line_text).group(1)
            date_time = re.search('[A-Z][a-z][a-z] +[0-3]*[0-9] [0-5][0-9]:[0-5][0-9]:[0-5][0-9]', line_text).group(0)
        except Exception as e:
            continue

        if user in ignored_names:
            continue

        date_with_year = str(year) + date_time

        date_object = datetime.strptime(date_with_year, "%Y %b %d %H:%M:%S")

        if options.minute_format:
            date_object = date_object.replace(second=0, microsecond=0)
        elif options.hour_format:
            date_object = date_object.replace(minute=0, second=0, microsecond=0)

        build_user_list(user, date_object, user_lists, output_dir)

    build_user_files(user_lists, output_dir, options)
    build_total_file(user_lists, output_dir, options)


def parse_output_file(filename, aggregate_list):
    f = open(filename, 'r')

    for line in f:
        line_text = str(line)
        date_value, time_value, count = line_text.split(',')
        key = date_value + time_value
        count = count.lstrip()
        if key not in aggregate_list:
            aggregate_list[key] = count
        else:
            aggregate_list[key] += count

    f.close()


def build_user_files(user_lists, directory='./', options=None):
    if options.minute_format:
        time_format = 60
    elif options.hour_format:
        time_format = 60 * 60
    else:
        time_format = 1
    if not os.path.exists(directory):
        os.makedirs(directory)
 
    for user in user_lists: 
        current_user_list = {}
        f = open(directory + user + '.parserout.tmp', 'r')
        for date_and_time in f:
            date_and_time = date_and_time.rstrip()
            date_and_time = datetime.strptime(date_and_time, "%Y-%m-%d %H:%M:%S")
            if date_and_time not in current_user_list:
                current_user_list[date_and_time] = 1
            else:
                current_user_list[date_and_time] += 1
        f.close()

        os.remove(directory + user + '.parserout.tmp')
     
        if options.zeroes:
            current_user_list = add_zeroes(current_user_list, time_format)
    
        current_user_list = formatted(current_user_list, time_format)

        f = open(directory + user + '.parserout', 'a+')
        for item in sorted(current_user_list):
            f.write(str(item.rstrip()) + ', ' + str(current_user_list[item]) + '\n')
        f.close()


def formatted(list_in, format_type):
    for key in list_in.keys():
        if format_type == 60:
            new_key = datetime.strftime(key, "%Y%m%d, %H:%M")
        elif format_type == 3600:
            new_key = datetime.strftime(key, "%Y%m%d, %H")
        else:
            new_key = datetime.strftime(key, "%Y%m%d, %H:%M:%S")
        list_in[new_key] = list_in.pop(key)
    return list_in


def build_total_file(user_lists, directory='./', options=None):
    if options.minute_format:
        time_format = 60
    elif options.hour_format:
        time_format = 60 * 60
    else:
        time_format = 1
    total_list = {} 
    for user in user_lists:
        f = open(directory + user + '.parserout', 'r')
        for line in f:
            date, time, count = line.split(',')
            time = time.lstrip()
            count = int(count.lstrip().rstrip())
            date_time_key = date + time
            if time_format == 60:
                date_and_time = datetime.strptime(date_time_key, "%Y%m%d%H:%M")
            elif time_format == 3600:
                date_and_time = datetime.strptime(date_time_key, "%Y%m%d%H")
            else:
                date_and_time = datetime.strptime(date_time_key, "%Y%m%d%H:%M:%S")

            if date_and_time not in total_list:
                total_list[date_and_time] = count
            else:
                total_list[date_and_time] += count
        f.close()

    if options.zeroes:
        total_list = add_zeroes(total_list, time_format)

    total_list = formatted(total_list, time_format)

    f = open(directory + 'all_users.parserout', 'w+')
    for key in sorted(total_list):
        f.write(str(key) + ', ' + str(total_list[key]) + '\n')
    f.close()


def build_total_aggregate_file(directory='.'):
    if not os.path.exists(directory):
        os.makedirs(directory)
    aggregate_list = {}
    for f in glob.glob(directory + '/*_parser_output/all_users.parserout'):
        parse_output_file(f, aggregate_list)
    f = open(directory + '/aggregate_all_logs.parserout', 'w+')
    for item in sorted(aggregate_list):
       date, time = item.split(' ')
       f.write(str(date) + ", " + time + ", " +str(aggregate_list[item]))
    f.close()


####################################################################################
# Build up a list of users found in the file and write connection instance to file #
####################################################################################
def build_user_list(user, time_key, user_lists, output_dir='./'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # write found times to a temp file to be parsed later
    f = open(output_dir + user + '.parserout.tmp', 'a+')
    f.write(str(time_key) + '\n')
    f.close()

    # add user to a dictionary instead of array for constant retrieval
    if user not in user_lists:
        user_lists[user] = 0


#####################################################
# Add zeroes at times where no connections occurred #
#####################################################
def add_zeroes(time_list, time_format=1):
    # Need to sort keys to accurately get first and last connection times
    sorted_keys = sorted(time_list.keys())
    first_element = sorted_keys[0]
    last_element = sorted_keys[-1]
    zero_list = {}
    
    # iterate through found times from first to last, maintain separate list of times with no connections
    while first_element < last_element:
        if first_element not in time_list:
            zero_list[first_element] = 0
        # increment time value up depending on format specified by user. Defaults to 1 second
        first_element = first_element + timedelta(0, time_format)
    total = time_list.copy()
    total.update(zero_list)
    return total


####################################
# Remove all previous output files #
####################################
def remove_previous_output(directory):
    print directory
    for output_file in glob.glob(directory + '/**/*.parserout'):
        os.remove(output_file)


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

    if options.total:
        build_total_aggregate_file(output_folder)


if __name__ == "__main__":
    main()
