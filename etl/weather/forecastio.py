#!/usr/bin/python

import datetime
import json
import os
import subprocess
import urlfetch

class ForecastIOImport(object):
    """API to interface with forecast.io and get data into HDFS."""

    def __init__(self, secrets, data_dir):
        self.token = secrets.ForecastIOToken

        self.locations = [
            ['Mountain_View', 37.39535, -122.0890],
            ['Palo_Alto', 37.43837, -122.1536],
            ['Redwood_City', 37.48660, -122.2290],
            ['San_Francisco', 37.78770, -122.4016],
            ['San_Jose', 37.33641, -121.8916]
        ]

        self.data_dir = data_dir

    def do_import(self, start_date, end_date, max_requests):
        """Import historical weather data from forecast.io.

        Attempts to load data for 5 specific Bay Area locations for each day
        in the given range by fetching data from forecast.io.

        Importing stops when the entire date range has been covered or the
        given number of requests has been used up.

        Data is written to the data directory passed to the constructor under
        a subdirectory named by the specific date, as follows:

          DATA_DIR/YYYY-MM-DD/weather_LOCATION.json
        """
        try:
            os.mkdir("./%s" % self.data_dir)
        except OSError:
            pass

        current_date = start_date
        num_requests = 0
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            dir_name = "%s/%s" % (self.data_dir, date_str)

            locations_to_fetch = []
            for name, lat, lng in self.locations:
                filename = "%s/weather_%s.json" % (dir_name, name)
                if os.path.isfile(filename):
                    continue

                locations_to_fetch.append([name, lat, lng])

            if not locations_to_fetch:
                # Already fetched all locations for this date
                current_date += datetime.timedelta(days=1)
                continue

            if num_requests + len(locations_to_fetch) > max_requests:
                # Fetching requests for this date would put us over our
                # threshold
                break

            print "Requesting weather data for %s..." % current_date

            try:
                os.mkdir(dir_name)
            except OSError:
                pass

            for name, lat, lng in locations_to_fetch:
                print "Getting location %s..." % name
                url = (
                    "https://api.forecast.io/forecast/%s/%s,%s,%sT00:00:00" % (
                        self.token, lat, lng, date_str))
                filename = "%s/weather_%s.json" % (dir_name, name)
                data = urlfetch.get(url).content
                with open(filename, "w") as f:
                    f.write(data)
                num_requests += 1

            current_date += datetime.timedelta(days=1)

        print "Stopped requests at %s" % current_date

    def copy_to_hdfs(self):
        """Convert weather data imported in `do_import` to CSV and transform.

        The CSV is written to to HDFS and then transformed in Spark. The output
        is then put into the /weather_data_schema directory in Parquet format.
        """

        print "Converting weather data to CSV..."

        subprocess.call(
            "rm -rf /vagrant/tmp && mkdir /vagrant/tmp",
            shell=True)

        with open("/vagrant/tmp/weather.csv", "w") as out:
            for root, dir, files in os.walk(self.data_dir):
                for file in files:
                    if file.startswith("weather_"):
                        date = root[len(self.data_dir)+1:len(self.data_dir)+11]
                        city = file[8:-5]

                        with open("%s/%s" % (root, file)) as f:
                            data = json.load(f)

                        if not data:
                            print "ERROR in %s/%s" % (root, file)
                            continue

                        if ('hourly' not in data or data['hourly'] is None or
                            'data' not in data['hourly']):
                            print "ERROR in %s/%s" % (root, file)
                            print data
                            continue

                        for entry in data['hourly']['data']:
                            row = [
                                date,
                                city,
                                data['latitude'],
                                data['longitude'],
                                datetime.datetime
                                    .fromtimestamp(entry['time'])
                                    .strftime("%H:%M"),
                                entry['summary'],
                                entry.get('precipIntensity', ''),
                                entry.get('precipProbability', ''),
                                entry.get('temperature', ''),
                                entry.get('apparentTemperature', ''),
                                entry.get('humidity', ''),
                                entry.get('windSpeed', ''),
                                entry.get('visibility', ''),
                                entry.get('pressure', ''),
                            ]
                            out.write(",".join([str(x) for x in row]) + "\n")

        print "Copying file to HDFS..."

        # Copy the CSV into HDFS
        subprocess.call(
            "/vagrant/hadoop/hadoop-hdfs.sh "
            "dfs -rm hdfs://hadoop:9000/weather.csv",
            shell=True
        )
        subprocess.call(
            "/vagrant/hadoop/hadoop-hdfs.sh "
            "dfs -put /vagrant/tmp/weather.csv hdfs://hadoop:9000/",
            shell=True
        )

        subprocess.call("rm -rf /vagrant/tmp", shell=True)

        print "Running transform in Spark... (This can take a looong time.)"

        # Run transform in Spark
        subprocess.call(
            '/vagrant/spark/run-pyspark-cmd.sh '
            '/vagrant/etl/spark-weather-transform.py',
            shell=True
        )

        print "Done."
