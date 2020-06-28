# CYB548 - simple dataset upload to Google Sheets

This script takes a .csv dataset from Kaggle, parses it, and inserts it into an SQLite database with some help from the Pandas library. From there, a connection to Google Sheets is made using the gspread and oauth2 libraries. Finally, the data is read from the database and written to Google Sheets.

# CYB551 - data vizualization using matplotlib

This script takes a .pcap file and breaks it down by packet using Scapy to reveal source IPs, destination IPs, protocols, souce ports, and destination ports. From there, a Pandas dataframe is contructed from the details and a dictionary is constructed.

The Prettytable library uses the aforementioned dictionary to create a table listing the most common source IPs.

The Matplotlib library is used to create a visualization of the table.
