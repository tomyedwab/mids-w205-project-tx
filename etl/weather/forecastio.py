#!/usr/bin/python

import datetime
import os
import subprocess
import urlfetch

class ForecastIOImport(object):
    """API to interface with forecast.io and get data into HDFS."""

    def __init__(self, secrets, root_dir):
        self.token = secrets.ForecastIOToken

        self.locations = [
            ['Mountain_View', 37.39535, -122.0890],
            ['Palo_Alto', 37.43837, -122.1536],
            ['Redwood_City', 37.48660, -122.2290],
            ['San_Francisco', 37.78770, -122.4016],
            ['San_Jose', 37.33641, -121.8916]
        ]

        self.root_dir = root_dir

    def do_import(self, start_date, end_date, max_requests):
        """Import historical weather data from forecast.io.

        Attempts to load data for 5 specific Bay Area locations for each day
        in the given range by fetching data from forecast.io.

        Importing stops when the entire date range has been covered or the
        given number of requests has been used up.

        Data is written to the root directory passed to the constructor under
        a subdirectory named by the specific date, as follows:

          ROOT_DIR/YYYY-MM-DD/weather_LOCATION.json
        """
        try:
            os.mkdir("./%s" % self.root_dir)
        except OSError:
            pass

        current_date = start_date
        num_requests = 0
        while current_date <= end_date:
            dir_name = "%s/%s" % (
            self.root_dir, current_date.strftime("%Y-%m-%d"))

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
                os.mkdir("./%s" % dir_name)
            except OSError:
                pass

            for name, lat, lng in locations_to_fetch:
                print "Getting location %s..." % name
                url = (
                    "https://api.forecast.io/forecast/%s/%s,%s,%sT00:00:00" % (
                        self.token, lat, lng, dir_name))
                filename = "%s/weather_%s.json" % (dir_name, name)
                data = urlfetch.get(url).content
                with open(filename, "w") as f:
                    f.write(data)
                num_requests += 1

            current_date += datetime.timedelta(days=1)

        print "Stopped requests at %s" % current_date

    def copy_to_hdfs(self):
        """Copy weather data imported in `do_import` to HDFS."""
        print "Copying all files to HDFS... (May take a while)"

        # Make the directory if it doesn't already exist
        subprocess.call(
            "/vagrant/hadoop/hadoop-hdfs.sh "
            "dfs -mkdir -p hdfs://hadoop:9000/weather",
            shell=True
        )
        # Copy all subdirectories we've written to above into HDFS
        subprocess.call(
            "/vagrant/hadoop/hadoop-hdfs.sh "
            "dfs -put %s/20* hdfs://hadoop:9000/weather/" % self.root_dir,
            shell=True
        )

        print "Done."
