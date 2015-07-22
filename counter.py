#!/usr/bin/env python

import re
import os
import glob
from datetime import datetime, date
import argparse
import copy
import gc
import time


####################################
# Remove all previous output files #
####################################
def remove_previous_output(directory):
    for output_file in glob.glob(directory + '/**/*.out'):
        os.remove(output_file)


##########################################
# Build up aggregate of every log parsed #
##########################################
def build_total_aggregate_file(output_folder):
    total_aggregate = {}
    # this value will be added to the count of all timestamps in successive log files. Set to zero for first log.
    last_count_from_previous_log = 0
    # So we know if we just started parsing a new log file
    first_line = True
    for filename in glob.glob(output_folder + '/**/*/all_users.out'):
        with open(filename, 'r') as f:
            for line in f:
                # Parsing output files in format Date, time, count
                day, time_stamp, count = line.split(',')
                time_stamp = time_stamp.lstrip()
                count = int(count.strip())

                date_object = datetime.strptime(str(day)+str(time_stamp), "%Y%m%d%H:%M:%S")
                time_stamp = int(time.mktime(date_object.timetuple()))

                # This if block checks if the first line in a log is accounting for connections in a previous file.
                # If it is, forget about the last count of the previous log, use new log's modified count instead.
                # It also pads out times between log files.
                # Example: log one ended at 12 am, log two began at 1pm.
                # Pad out aggregate output from 12am to 1pm with log one count at 12am.
                if first_line:
                    if len(total_aggregate.keys()) > 0:
                        # Index to iterate through
                        index = sorted(total_aggregate.keys())[-1]
                        # Pad the gap between both log files
                        while index + 1 < time_stamp:
                            total_aggregate[index + 1] = total_aggregate[index]
                            index += 1
                    # If the log accounts for previously closed connections
                    if count != 1:
                        # Then we want that log file's count to be used instead of the last log's last count
                        last_count_from_previous_log = 0
                    first_line = False

                # Every connection time count in the aggregate file is increased by the amount of open connections
                # left behind by the previous log
                if time_stamp not in total_aggregate:
                    total_aggregate[time_stamp] = last_count_from_previous_log
                total_aggregate[time_stamp] += count
        f.close()
        # Reset our values to loop through a new file
        last_count_from_previous_log = total_aggregate[sorted(total_aggregate.keys())[-1]]
        first_line = True

    # Write aggregate file
    with open(output_folder+'all_logs.out', 'w+') as f:
        for time_stamp in sorted(total_aggregate):
            # Set resolution to seconds for output file
            f.write(format_date(time_stamp, 1) + ", " + str(total_aggregate[time_stamp]) + "\n")
    f.close()


##################################################
# Change Unix timestamp to correct output format #
##################################################
def format_date(unix_timestamp, resolution):
    # Change output depending on resolution option
    if resolution == 60:
        date_format = "%Y%m%d, %H:%M"
    elif resolution == 3600:
        date_format = "%Y%m%d, %H"
    else:
        date_format = "%Y%m%d, %H:%M:%S"

    # make a date object out of the unix timestamp
    date_object = time.localtime(unix_timestamp)

    # convert that object to a formatted string
    return time.strftime(date_format, date_object)


####################################################################################
# Parse a single .log file, call functions to generate user files + total log file #
####################################################################################
def parse_file(file_name, output_folder, options):

    # user: {timestamp: count} nested dictionary which contains the timestamp: count values for each user found
    user_connections = {}

    # timestamp: count dictionary for open connections across all users
    total_connections_for_log = {}

    # a list of connections closed that we did not open, meaning they were opened in a previous log
    old_connections = []

    # user: count dictionary to get the count of open connections for a user at some time
    user_counts = {}

    # pid: user dictionary to represent "ownership" of a process id
    process_ids = {}

    resolution = 1

    # default to current year if no year given by user
    year = options.year or str(date.today().year)

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

                # We don't touch counts, but still give ignored user "ownership" of that process id for when we close it
                if user in ignored_users:
                    process_ids[pid] = user
                    continue

                # We only wants hours and minutes
                if resolution == 60:
                    date_time = date_time[:-3]
                # We only want hours
                elif resolution == 3600:
                    date_time = date_time[:-6]

                # Log files don't contain the year. prepend time with current year or year specified by user
                opened_time = year + " " + date_time
                # Convert to date object
                date_object = datetime.strptime(opened_time, date_format)
                # Convert that date object to an accurate unix timestamp
                time_stamp = int(time.mktime(date_object.timetuple()))

                # Keep track of greatest time found in log for use later
                if time_stamp > greatest_time:
                    greatest_time = time_stamp

                # increment user's open connection count by 1
                if user not in user_counts:
                    user_counts[user] = 0

                user_counts[user] += 1

                # set user's count at this timestamp to whatever their current open connection count value is
                if user not in user_connections:
                    user_connections[user] = {}

                user_connections[user][time_stamp] = int(user_counts[user])

                # give user ownership of this process id
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

                # If this connection was opened by us
                if pid in process_ids:

                    # get user by process id ownership since exit line does not contain user
                    user = process_ids[pid]

                    # do nothing to counts if ignored user, but remove user's ownership of process id
                    if user in ignored_users:
                        del process_ids[pid]
                        continue

                    # decrement user's open connection count
                    user_counts[user] -= 1

                    # same process as in an open connection
                    user_connections[user][time_stamp] = int(user_counts[user])

                    # remember to remove ownership of process id so it can be used again
                    del process_ids[pid]

                # if the process id is not in our dictionary, we did not open it. It was opened before this log began
                else:
                    old_connections.append(time_stamp)

            # we don't care about the line if it is not an opening or closing of a connection
            else:
                continue
    f.close()

    # Clear out process ids for memory optimization.
    process_ids.clear()
    gc.collect()

    # make a copy to iterate through so "size of dictionary changed during iteration" doesn't happen when
    # we delete a user_connections[user] entry
    user_names = copy.copy(user_connections.keys())

    for username in user_names:
        # make a copy of old connections each iteration since we pop values from the list
        old_connections_copy = copy.copy(old_connections)
        pad_user_times(output_folder, username, total_connections_for_log, user_connections, old_connections_copy, resolution, greatest_time)

    write_all_users(output_folder, total_connections_for_log, resolution)


##################################################
# Pad out times where connections were sustained #
##################################################
def pad_user_times(output_folder, username, total_connections_for_log, user_connections, old_connections, resolution, greatest_time):
    with open(output_folder + username + '.out', 'w+') as f:
        # iterate sequentially through user's timestamps
        for timestamp in sorted(user_connections[username]):
            # timestamps not in the list are timestamps where no changes in connections occurred.
            # we want to put those timestamps in and set them equal to the last timestamp where a change occured.
            # we want to do this until we hit the greatest timestamp found in the log so every user has the same
            # end time
            while timestamp + resolution not in user_connections[username] and timestamp + resolution <= greatest_time:
                user_connections[username][timestamp+resolution] = user_connections[username][timestamp]
                # iterate 1, 60 or 3600 seconds based on option
                timestamp += resolution

        # add user times to total(all_users) dictionary
        for timestamp in sorted(user_connections[username]):

            # if the current timestamp is greater than the earliest "old_connection" closed,
            # pop the list until it's not
            while len(old_connections) > 0 and old_connections[0] <= timestamp:
                old_connections.pop()

            # initialize open connections at timestamp to num of old connections still open at this time
            if timestamp not in total_connections_for_log:
                total_connections_for_log[timestamp] = len(old_connections)
            # add num of user_connections at this time to total connections at this time
            total_connections_for_log[timestamp] += user_connections[username][timestamp]

            # write user connection
            f.write(format_date(timestamp, resolution) + ", " + str(user_connections[username][timestamp]) + "\n")

        # we are done with the user. free memory since the dictionary with padded out times gets huge.
        del user_connections[username]
        gc.collect()


######################################
# Write all user connections to file #
######################################
def write_all_users(output_folder, total_connections_for_log, resolution):

    with open(output_folder + 'all_users.out', 'w+') as f:
        for timestamp in sorted(total_connections_for_log):
            f.write(format_date(timestamp, resolution) + ", " + str(total_connections_for_log[timestamp]) + "\n")

    # free up memory
    total_connections_for_log.clear()
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

    if options.total:
        build_total_aggregate_file(output_folder)


if __name__ == "__main__":
    main()
