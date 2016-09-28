from configparser import ConfigParser
from luigi.contrib import sqla
from luigi.mock import MockFile
from googleplaces import GooglePlaces
from sqlalchemy import Float, DateTime, Integer, String
import csv
import logging
import luigi
import os
import requests
import shutil


class DownloadTaxiUrls(luigi.Task):
    """ Download NYC Taxi Data for our use. """
    year = luigi.IntParameter(default=2016)
    months = luigi.ListParameter(default=[6,7,8])
    url_list = luigi.Parameter(default='https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/raw_data_urls.txt')
    cab_type = luigi.Parameter(default='yellow')

    def run(self):
        resp = requests.get(self.url_list)
        urls = []
        possible_strs = ['{}-{:02d}'.format(self.year, m) for m in self.months]
        for line in resp.iter_lines():
            if self.cab_type in str(line):
                for datestr in possible_strs:
                    if datestr in str(line):
                        urls.append(line)
                        break
        with self.output().open('w') as url_file:
            for url in urls:
                print(url.decode(), file=url_file)

    def output(self):
        return luigi.LocalTarget('/tmp/taxi_data/urls.txt')


class DownloadTaxiData(luigi.Task):
    """ Downloading each file of taxi data for each url from the repo. """
    def requires(self):
        return DownloadTaxiUrls()

    def input(self):
        return luigi.LocalTarget('/tmp/taxi_data/urls.txt')

    def run(self):
        for url in self.input().open('r'):
            yield DownloadTaxiFile(url.rstrip('\n'))


class DownloadTaxiFile(luigi.Task):
    """ Download each file, and save it locally to /tmp/taxi_data """
    url = luigi.Parameter()

    def requires(self):
        return DownloadTaxiUrls()

    def run(self):
        file_name = self.url.split('/')[-1]
        resp = requests.get(str(self.url), stream=True)
        with open(self.output().path, 'wb') as taxi_file:
            shutil.copyfileobj(resp.raw, taxi_file)

    def output(self):
        file_name = self.url.split('/')[-1]
        return luigi.LocalTarget('/tmp/taxi_data/{}'.format(file_name))


class AddTaxiLocations(luigi.Task):
    """ Import the files and add the locations using Google Reverse Search. """
    dir_name = luigi.Parameter(default='/tmp/taxi_data/')

    def requires(self):
        return DownloadTaxiData()

    def input(self):
        return [luigi.LocalTarget('/tmp/taxi_data/{}'.format(fn)) for fn
                in os.listdir(self.dir_name) if fn.endswith('csv')]

    def run(self):
        for fn in self.input():
            rdr = csv.DictReader(fn.open('r'))
            for line in rdr:
                yield AddTaxiLocation(line)


class AddTaxiLocation(luigi.Task):
    """ Search for pickup and dropoff location and add them via Google API. """
    line = luigi.DictParameter(default={})

    columns = ['vendor_name', 'Rate_Code', 'surcharge', 'store_and_forward',
               'mta_tax', 'Total_Amt', 'Fare_Amt', 'Tolls_Amt', 'Tip_Amt',
               'Trip_Pickup_DateTime', 'Trip_Dropoff_DateTime',
               'Passenger_Count', 'Payment_Type', 'Trip_Distance',
               'Start_Lon', 'Start_Lat', 'End_Lon', 'End_Lat',
               'pickup_location_name', 'pickup_location_phone',
               'pickup_location_addy', 'pickup_location_web',
               'dropoff_location_name', 'dropoff_location_phone',
               'dropoff_location_addy', 'dropoff_location_web']


    def add_addy_info(self, res, loc_type):
        if len(res.places):
            place = res.places[0]
            place.get_details()
            self.line['{}_location_name'.format(loc_type)] = place.name
            self.line['{}_location_phone'.format(loc_type)] = place.local_phone_number
            self.line['{}_location_addy'.format(loc_type)] = place.vicinity
            self.line['{}_location_web'.format(loc_type)] = place.website

    def run(self):
        self.line = dict((k, v) for k,v in self.line.items())
        config = ConfigParser()
        config.read('../config/prod.cfg')
        client = GooglePlaces(config.get('google', 'api_key'))
        res = client.nearby_search(lat_lng={'lat': self.line['Start_Lat'],
                                            'lng': self.line['Start_Lon']})
        self.add_addy_info(res, 'pickup')
        res = client.nearby_search(lat_lng={'lat': self.line['End_Lat'],
                                            'lng': self.line['End_Lon']})
        self.add_addy_info(res, 'dropoff')
        with self.output().open('w') as line_output:
            line_with_tabs = '\t'.join([self.line.get(key) if self.line.get(key)
                                        else '' for key in self.columns])
            line_output.write(line_with_tabs)

    def output(self):
        return MockFile("AddTaxiLocation")


class SaveTaxiRow(sqla.CopyToTable):
    """ Save each taxi line with the location information to the database """
    connection_string = 'sqlite:///taxi_db.db'
    table = 'taxi_rides'
    columns = [
        (['vendor_name', String(10)], {}),
        (['rate_code', String(4)], {}),
        (['surcharge', Float()], {}),
        (['store_and_forward', Integer()], {}),
        (['mta_tax', Float()], {}),
        (['total_amt', Float()], {}),
        (['fare_amt', Float()], {}),
        (['tolls_amt', Float()], {}),
        (['tip_amt', Float()], {}),
        (['pickup_datetime', DateTime()], {}),
        (['dropoff_datetime', DateTime()], {}),
        (['passenger_count', Integer()], {}),
        (['payment_type', String(100)], {}),
        (['trip_distance', Float()], {}),
        (['pickup_longitude', Float()], {}),
        (['pickup_latitude', Float()], {}),
        (['dropoff_longitude', Float()], {}),
        (['dropoff_latitude', Float()], {}),
        (['pickup_location_name', String(128)], {}),
        (['pickup_location_phone', String(64)], {}),
        (['pickup_location_addy', String(256)], {}),
        (['pickup_location_web', String(64)], {}),
        (['dropoff_location_name', String(128)], {}),
        (['dropoff_location_phone', String(64)], {}),
        (['dropoff_location_addy', String(256)], {}),
        (['dropoff_location_web', String(64)], {}),
    ]

    def requires(self):
        return AddTaxiLocation()
