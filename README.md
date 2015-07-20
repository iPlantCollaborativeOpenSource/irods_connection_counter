# irods_connection_counter
This project counts individual user connections from irods and outputs individual user file logs as well as a total connection count for each .log file.

#Guide
parser.py <.log file or directory containing .log files>

#Options
--hours for connections by hour

-m for connections by minute

By default, the log will output connections by second

-i "name1,name2,name3" for users you want to ignore

-y <year> for the year the log was generated. Defaults to current year

-t to generate a total aggregate file of all logs parsed

#Example
python parser.py . -o all -t --hours -i "joseph"

This will run the log parser on the current working directory. All .log files found have output generated in the "all" directory. We want the resolution to be in hours, and we want to ignore user "joseph"


#Output
The script will create an output folder for each individual .log file titled <name of log>_parser_output. 
Each folder will contain a file for every user found in the log as well as a "parser_total.out" file containing the aggregate of all user connections.

The format for Jun 16 08:10:17 would be

20150616, 08:10:17

20150616, 08:10

20150616, 08

Depending on the time formatting options described above.
