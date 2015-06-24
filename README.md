# irods_connection_counter
This project counts individual user connections from irods and outputs individual user file logs as well as a total connection count for each .log file.

#Guide
parser.py <.log file or directory containing .log files>

#Options
-l for connections by hour

-m for connections by minute

-i "name1,name2,name3" for users you want to ignore

By default, the log will output connections by second and will count all users in the file

#Output
The script will create an output folder for each individual .log file titled <name of log>_parser_output. 
Each folder will contain a file for every user found in the log as well as a "parsertotal.out" file containing the aggregate of all user connections.

The format for Jun 16 08:10:17 would be
20150616 08:10:17

20150616 08:10

20150616 08

Depending on the -l or -m options described above.
