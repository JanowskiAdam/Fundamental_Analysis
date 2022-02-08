# Fundamental_Analysis
Project Extracting Financial Statements of multiple stocks, Transforming data and Loading data to SQLite where all Financial Statements are stored. Later Data is retrieved from Database to DataFrame in Python where will be further processed.

Main idea is to create set of rules on which investing decisions will be made based on financial statements.

Files
========================================================================
FS_Downloader.py
Connection to Yahoo Finance Api.
FS_Downloader class downloads full financial statement of chosen company and concate them toghether.

FS_SQL.py
Connection to DataBase where financial Statements are stored.
FS_SQL class using FS_Downloader save financial statements into database and opposite way retrieved financial statements from database back to dataframe for futher processing.

Financial_Statement_Analysis.py
Created financial indicators from financial statements
FA class creted and displays on chart financial indicators based on retrieved financial statements. FA class is not yet connected to other classe and constitutes an independent class.
