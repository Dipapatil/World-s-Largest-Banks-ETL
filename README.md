# World-s-Largest-Banks-ETL
This project scrapes data from a website to extract information about the top 10 largest banks using Python. It transforms the data to include additional fields, such as Market Cap in EUR and INR, and stores the data in both a CSV file and a database table. Additionally, it generates a log file that tracks the timestamps of each process.
I have completed this Python project as the part of IBM Data Engineering Professional Certificate.



## This Project is completed on Coursera's cloud platform as well as locally on Pycharm
## Tools and Packages Used
Pycharm, Pandas, Requests, sqlite, BeautifulSoup

## Process
This python project scrapes this [website](https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks) to get top 10 largest banks of world, then stores data into pandas dataframe and transforms data to include few more columns that shows Market Cap in EUR, GBP and INR.
Transforms dataframe then loaded into csv file and sqlite database table to query data.
Additionally log tracking process is included in all phases of ETL.


## Python Script
[World's Largest Banks - ETL Script](https://github.com/Dipapatil/World-s-Largest-Banks-ETL/blob/main/worlds_largest_bank_etl.py)

## Output Log file and csv file
[Output CSV file](https://github.com/Dipapatil/World-s-Largest-Banks-ETL/blob/main/top_10_banks.csv)

[Log File With Timestamps](https://github.com/Dipapatil/World-s-Largest-Banks-ETL/blob/main/top_10_bank_logs_timestamp.txt)

![Screenshot of log file](https://github.com/Dipapatil/World-s-Largest-Banks-ETL/blob/main/Task_7_log_content.png)
![Screenshot of sql queries](https://github.com/Dipapatil/World-s-Largest-Banks-ETL/blob/main/task_6_sql.png)


