#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""The following application is based on the work from:
   http://www.ryanpraski.com/python-google-analytics-api-unsampled-data-multiple-profiles/

   This application demonstrates how to use the python client library to access
   Google Analytics data and load it to MapD's database (www.mapd.com).
"""

"""
This application demonstrates how to use the python client library to access
Google Analytics data. The sample traverses the Management API to obtain the
authorized user's first profile ID. Then the sample uses this ID to
contstruct a Core Reporting API query to return the top 25 organic search
terms.

Before you begin, you must sigup for a new project in the Google APIs console:
https://code.google.com/apis/console

Then register the project to use OAuth2.0 for installed applications.

Finally you will need to add the client id, client secret, and redirect URL
into the client_secrets.json file that is in the same directory as this sample.

Sample Usage:

  $ python hello_analytics_api_v3.py

Also you can also get help on all the command-line flags the program
understands by running:

  $ python hello_analytics_api_v3.py --help
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

import argparse
import sys
import csv
import string
import os
import re
import pandas as pd
import numpy as np
# MAPD Modules
from mapd_utils import *

from apiclient.errors import HttpError
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

class SampledDataError(Exception): pass

# original set - key_dimensions=['ga:date','ga:hour','ga:minute','ga:networkLocation','ga:browserSize','ga:browserVersion']
#original set - all_dimensions=['ga:userAgeBracket','ga:userGender','ga:country','ga:countryIsoCode','ga:city','ga:continent','ga:subContinent','ga:userType','ga:sessionCount','ga:daysSinceLastSession','ga:sessionDurationBucket','ga:referralPath','ga:browser','ga:operatingSystem','ga:browserSize','ga:screenResolution','ga:screenColors','ga:flashVersion','ga:javaEnabled','ga:networkLocation','ga:mobileDeviceInfo','ga:mobileDeviceModel','ga:mobileDeviceBranding','ga:deviceCategory','ga:language','ga:adGroup','ga:source','ga:dataSource','ga:sourceMedium','ga:adSlot','ga:mobileInputSelector','ga:mobileDeviceMarketingName','ga:searchCategory','ga:searchDestinationPage','ga:interestAffinityCategory','ga:landingPagePath','ga:exitPagePath','ga:browserVersion','ga:eventLabel','ga:eventAction','ga:eventCategory','ga:hour','ga:yearMonth','ga:Month','ga:date','ga:keyword','ga:campaign','ga:adContent']

key_dimensions=['ga:date','ga:hour','ga:minute','ga:longitude','ga:latitude','ga:landingPagePath']
all_dimensions=['ga:networkLocation', 'ga:country', 'ga:city', 'ga:medium', 'ga:source', 'ga:sessionDurationBucket', 'ga:sessionCount', 'ga:deviceCategory', 'ga:campaign', 'ga:adContent','ga:keyword']
n_dims = 7 - len(key_dimensions)

def get_service():
  """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account JSON key file.

  Returns:
    A service that is connected to the specified API.
  """
  scope = ['https://www.googleapis.com/auth/analytics.readonly']
  key_file_location = './client_secrets.json'
  api_name = 'analytics'
  api_version = 'v3'
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      key_file_location, scopes=scope)

  # Build the service object.
  service = build(api_name, api_version, credentials=credentials)

  return service


# Traverse the GA management hierarchy and construct the mapping of website 
# profile views and IDs.
def traverse_hierarchy(argv):
  # Authenticate and construct service.
  service = get_service()

  try:
    accounts = service.management().accounts().list().execute()
    for account in accounts.get('items', []):
      accountId = account.get('id')
      webproperties = service.management().webproperties().list(
          accountId=accountId).execute()
      for webproperty in webproperties.get('items', []):
        firstWebpropertyId = webproperty.get('id')
        profiles = service.management().profiles().list(
            accountId=accountId,
            webPropertyId=firstWebpropertyId).execute()
        for profile in profiles.get('items', []):
          profileID = "%s" % (profile.get('id'))
          profileName = "%s_%s" % (webproperty.get('name'),profile.get('name'))
          profile_ids[profileName] = profileID

  except TypeError as error:
    # Handle errors in constructing a query.
    print(('There was an error in constructing your query : %s' % error))

  except HttpError as error:
    # Handle API errors.
    print(('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason())))

  except AccessTokenRefreshError:
    print ('The credentials have been revoked or expired, please re-run'
           'the application to re-authorize')

def merge_tables():
  global final_csv_file
  for i in range(0,len(csv_list),1):
    print(csv_list[i])
    if i == (len(csv_list) - 1):
      print("exiting for loop ...")
      break
  
    if i == 0:
      df1 = pd.read_csv(csv_list[0])
      df2 = pd.read_csv(csv_list[1])
    else:
      df1 = pd.read_csv('./data/combo.csv')
      os.system("rm -f ./data/combo.csv")
      df2 = pd.read_csv(csv_list[i+1])
  
    df1 = df1.dropna(subset = ['ga_longitude', 'ga_latitude'])
    df2 = df2.dropna(subset = ['ga_longitude', 'ga_latitude'])
    combo = pd.merge(df1, df2, how='left')
    df = pd.DataFrame(combo)
    df = df[df.ga_pageviews != 0]
    df = df[df.ga_longitude != 0]
    df = df[df.ga_latitude != 0]
    #df['ga_pageviews'] = df['ga_pageviews'].fillna(0)
    df = df.fillna("None")
    df.to_csv('./data/combo.csv', index=False)
    #os.system("sed -i '1,$s/,$/,None/' ./data/combo.csv")
    #os.system("sed -i '1,$s/,,/,None,/g' ./data/combo.csv")
    del df1
    del df2
    del combo
  
  df = pd.read_csv('./data/combo.csv')
  #df = pd.DataFrame(df1)
  df.ga_date = df.ga_date.apply(str)
  df.ga_hour = df.ga_hour.apply(str)
  df.ga_minute = df.ga_minute.apply(str)
  df['ga_date'] = df['ga_date'].str.replace(r'(\d\d\d\d)(\d\d)(\d\d)', r'\2/\3/\1')
  df['ga_date'] = df['ga_date'].astype(str) +" " + df['ga_hour'].astype(str) +":" +df['ga_minute'].astype(str) +":00"
  df = df.drop('ga_hour', axis=1)
  df = df.drop('ga_minute', axis=1)
  df.to_csv(final_csv_file, index=False)  
  print(df.head(3))
  print(df.isnull().sum())
  
def main(argv):
  global csv_list
  i = 0
  for dim_i in range(0,len(all_dimensions), n_dims):
    dimss = key_dimensions + all_dimensions[dim_i:dim_i+n_dims]
    dims = ",".join(dimss)
    #path = os.path.abspath('.') + '/data/'
    path = './data/'
    if not os.path.exists(path):
      os.mkdir(path, 0o755)
    file_suffix = '%s' % (all_dimensions[dim_i:dim_i+n_dims])
    file_suffix = '%s' % (file_suffix.strip('\[\'\]'))
    file_suffix = '%s' % (file_suffix[3:])
    filename = '%s_%s.csv' % (profile.lower(), file_suffix)
    filename = filename.strip()
    table_names[dims] = '%s_%s' % (profile.lower(), file_suffix)
    #table_filenames[dims] = path + '%s_%s.csv' % (profile.lower(), file_suffix)
    table_filenames[dims] = path + filename
    csv_list = csv_list + [table_filenames[dims]]
    i += 1
    files[dims] = open(path + filename, 'wt')
    writers[dims] = csv.writer(files[dims], lineterminator='\n')

  # Authenticate and construct service.
  service = get_service()  

  # Try to make a request to the API. Print the results or handle errors.
  try:
    profile_id = profile_ids[profile]
    if not profile_id:
      print('Could not find a valid profile for this user.')
    else:
      for start_date, end_date in date_ranges:
        for dim_i in range(0,len(all_dimensions), n_dims):
          dimss = key_dimensions + all_dimensions[dim_i:dim_i+n_dims]
          dims = ",".join(dimss)
          print(dims)
          limit = ga_query(service, profile_id, 0,
                                   start_date, end_date, dims).get('totalResults')
          print("Found " + str(limit) + " records") #VS
          for pag_index in range(0, limit, 10000):
            results = ga_query(service, profile_id, pag_index,
                                       start_date, end_date, dims)
            if results.get('containsSampledData'):
              raise SampledDataError #VS
            save_results(results, pag_index, start_date, end_date, dims)
          files[dims].close()

  except TypeError as error:
    # Handle errors in constructing a query.
    print(('There was an error in constructing your query : %s' % error))

  except HttpError as error:
    # Handle API errors.
    print(('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason())))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print ('The credentials have been revoked or expired, please re-run '
           'the application to re-authorize')

  except SampledDataError:
    # force an error if ever a query returns data that is sampled!
    print ('Error: Query contains sampled data!')


def ga_query(service, profile_id, pag_index, start_date, end_date, dims):

  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date=start_date,
      end_date=end_date,
      metrics='ga:pageviews',
      dimensions=dims,
      sort='-ga:pageviews',
      samplingLevel='HIGHER_PRECISION',
      start_index=str(pag_index+1),
      max_results=str(pag_index+10000)).execute()

# Write results reported from the Core Reporting API to the CSV file
def save_results(results, pag_index, start_date, end_date, dims):
  # New write header
  if pag_index == 0:
    if (start_date, end_date) == date_ranges[0]:
      print('Profile Name: %s' % results.get('profileInfo').get('profileName'))
      columnHeaders = results.get('columnHeaders')
      #cleanHeaders = [str(h['name']) for h in columnHeaders]
      #writers[dims].writerow(cleanHeaders)
      cleanHeaders_str = '%s' % [str(h['name']) for h in columnHeaders]
      cleanHeaders_str = cleanHeaders_str.replace(':', '_')
      cleanHeaders_str = cleanHeaders_str.replace('\'', '')
      cleanHeaders_str = cleanHeaders_str.replace('[', '')
      cleanHeaders_str = cleanHeaders_str.replace(']', '')
      cleanHeaders_str = cleanHeaders_str.replace(',', '')
      cleanHeaders = cleanHeaders_str.split()
      writers[dims].writerow(cleanHeaders)
    print('Now pulling data from %s to %s.' %(start_date, end_date))

  # Print data table.
  if results.get('rows', []):
    for row in results.get('rows'):
      for i in range(len(row)):
        old, new = row[i], str()
        for s in old:
          new += s if s in string.printable else ''
        row[i] = new
      writers[dims].writerow(row)

  else:
    print('No Rows Found')

  limit = results.get('totalResults')
  print(pag_index, 'of about', int(round(limit, -4)), 'rows.')
  return None

profile_ids = {}

#date_ranges = [('2017-08-27',
#               '2018-02-22')]
date_ranges = [('30daysAgo',
               'today')]

writers = {}
files = {}
table_names = {}
table_filenames = {}
csv_list = []

# Construct dictionary of GA website name and ids.
traverse_hierarchy(sys.argv)

# Select the GA profile view to extract data
selection_list = [0]
i = 1
print(('%5s %20s %5s %20s' % ("Item#", "View ID", " ", "View Name")))
for profile in sorted(profile_ids):
  selection_list = selection_list + [profile_ids[profile]]
  print(('%4s %20s %5s %20s' % (i, profile_ids[profile], " ", profile)))
  i +=1

print('Enter the item# of the view you would like to ingest into MapD: ', end=' ')
item = int(input())
if item == '' or item <= 0 or item >= len(selection_list):
  print(('Invalid selection - %s' % item))
  sys.exit(0)
print(('Item # %s selected' % item))

print('\nEnter the begin date and end date in the following format: YYYY-MM-DD YYYY-MM-DD')
print('Or hit enter to proceed with the default which is last 30 days data')
print('Date Range: ', end=' ')
begin_end_date = input()
if begin_end_date == '':
  print('Extract data from today to 30 days ago')
else:
  (begin_date, end_date) = [t(s) for t,s in zip((str, str), begin_end_date.split())]
  print(('Extract data from %s to %s' % (begin_date, end_date)))
  date_ranges = [(begin_date, end_date)]

print("\nEnter the MapD server information if you want to upload data,\n otherwise simply hit enter to use the manual procedure to upload the data")
print("  Information needed - <Hostname or IP Address> <db login> <db password> <database name> <SSH login>")
print('MapD Server Info: ', end=' ')
server_info = input()
if server_info == '':
  print('Use MapD Immerse import user interface to load the output CSV file')
  skip_mapd_connect = True
else:
  (mapd_host, db_login, db_password, database, ssh_login) = [t(s) for t,s in zip((str, str, str, str, str), server_info.split())]
  print(('The data from the selected view will be automatically uploaded to the %s database in the %s server' % (database, mapd_host)))
  skip_mapd_connect = False
print("")

for profile in sorted(profile_ids):
  if (selection_list[item] == profile_ids[profile]):
    print(('\nGoing to download data for %s (%s) ...' % (profile, profile_ids[profile])))
    table_name = profile.lower()
    table_name = '%s' % (table_name.replace(' ', ''))    
    final_csv_file = './data/%s.csv' % (table_name)    
    final_csv_gzfile = './data/%s.csv.gz' % (table_name)    
    main(sys.argv)
    # Merge the tables for the different dimensions.
    merge_tables()
print("Download of analytics data done.")

# Gzip the CSV file
import gzip
import shutil
if os.path.isfile(final_csv_gzfile):
  os.remove(final_csv_gzfile)
with open(final_csv_file, 'rb') as f_in, gzip.open(final_csv_gzfile, 'wb') as f_out:
    shutil.copyfileobj(f_in, f_out)

# Connect to MapD
if skip_mapd_connect == True:
  print("=======================================================================")
  print('Goto MapD Immerse UI and import the CSV file %s' % (final_csv_gzfile))
  print("=======================================================================")
  sys.exit(0)
connect_to_mapd(db_login, db_password, mapd_host, database)

# Load data into MapD table
load_to_mapd(table_name, final_csv_gzfile, mapd_host, ssh_login)
disconnect_mapd()
print("=======================================================================")
print('Goto MapD Immerse UI @ http://%s:9092/' % (mapd_host))
print("=======================================================================")

