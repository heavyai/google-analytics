#!/usr/bin/python
  
"""
  Module for connecting to MapD database and creating tables with the provided data.
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

if __name__ == "__main__":
  import argparse
  import sys
  import string
  import csv

import os
import pandas as pd
from pymapd import connect

connection = "NONE"

# Connect to the DB
def connect_to_mapd(str_user, str_password, str_host, str_dbname):
  global connection
  connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname)
  print (connection)

def drop_table_mapd(table_name):
  global connection
  command = 'drop table if exists %s' % (table_name)
  print (command)
  connection.execute(command)

def disconnect_mapd():
  global connection
  connection.close()

# Load CSV to Table using PyMapD
def load_table_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  table_name = table_name.replace('.', '_')
  df = pd.read_csv(csv_file)
  print (df.head(10))
  print (df.shape)
  drop_table_mapd(table_name)
  connection.create_table(table_name, df, preserve_index=False)
  connection.load_table(table_name, df, preserve_index=False)
  print (connection.get_table_details(table_name))

# Copy CSV to MapD server and load table using COPY
def copy_and_load_table_mapd(table_name, csv_file, mapd_host, mapd_user):
  global connection
  table_name = table_name.replace('.', '_')
  create_table_str = 'CREATE TABLE IF NOT EXISTS %s (ga_date TIMESTAMP, ga_longitude FLOAT, ga_latitude FLOAT, ga_landingPagePath TEXT ENCODING DICT(8), ga_networkLocation TEXT ENCODING DICT(8), ga_pageviews BIGINT, ga_country TEXT ENCODING DICT(8), ga_city TEXT ENCODING DICT(8), ga_medium TEXT ENCODING DICT(8), ga_source TEXT ENCODING DICT(8), ga_sessionDurationBucket BIGINT, ga_sessionCount BIGINT, ga_deviceCategory TEXT ENCODING DICT(8), ga_campaign TEXT ENCODING DICT(8), ga_adContent TEXT ENCODING DICT(8), ga_keyword TEXT ENCODING DICT(8))' % (table_name)
  print (create_table_str)
  connection.execute(create_table_str)
  server_csv_file = '/tmp/%s' % (os.path.basename(csv_file))
  command = 'scp %s %s@%s:%s' % (csv_file, mapd_user, mapd_host, server_csv_file)
  print (command)
  os.system(command)

  query = 'COPY %s from \'%s\' WITH (nulls = \'None\')' % (table_name, server_csv_file)
  print (query)
  connection.execute(query)
  print (connection.get_table_details(table_name))

