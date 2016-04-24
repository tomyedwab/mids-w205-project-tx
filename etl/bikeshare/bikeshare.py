import subprocess

# Sources for bikeshare data
year_1_data = 'https://s3.amazonaws.com/babs-open-data/babs_open_data_year_1.zip'
year_2_data =  'https://s3.amazonaws.com/babs-open-data/babs_open_data_year_2.zip'

class BikeshareImport(object):

    def __init__(self, ROOT_DIR, DATA_DIR):
        self.ROOT_DIR = ROOT_DIR
        self.DATA_DIR = DATA_DIR

    def do_import(self):
        """Concatenate the raw BikeShare data and transform.

        The concatenated CSVs are written to to HDFS and then transformed in
        Spark. The output is then put into the /station_data_schema and
        /status_data_schema directories in Parquet format.
        """

        print "Downloading and unzipping bikeshare data..."
        subprocess.call(
            "wget %s -O temp.zip && "
            "unzip -o -u temp.zip -d %s/bikeshare_raw && "
            "mv %s/bikeshare_raw/201402_babs_open_data/* %s/bikeshare_raw && "
            "mv %s/bikeshare_raw/201408_babs_open_data/* %s/bikeshare_raw && "
            "rmdir %s/bikeshare_raw/201402_babs_open_data && "
            "rmdir %s/bikeshare_raw/201408_babs_open_data && "
            "rm temp.zip" % (year_1_data, self.DATA_DIR, self.DATA_DIR, self.DATA_DIR,
                self.DATA_DIR, self.DATA_DIR, self.DATA_DIR, self.DATA_DIR),
            shell=True)

        subprocess.call(
            "wget %s -O temp.zip && "
            "unzip -o -u temp.zip -d %s/bikeshare_raw && "
            "rm temp.zip" % (year_2_data, self.DATA_DIR),
            shell=True)

        print "Processing bikeshare data and copying into HDFS..."

        # process station data  (works locally)
        subprocess.call('tail -n +2 %s/bikeshare_raw/201402_station_data.csv > %s/bikeshare_raw/station_data_1.csv' % (self.DATA_DIR, self.DATA_DIR), shell=True)
        subprocess.call('tail -n +2 %s/bikeshare_raw/201408_station_data.csv > %s/bikeshare_raw/station_data_2.csv' % (self.DATA_DIR, self.DATA_DIR), shell=True)
        subprocess.call('tail -n +2 %s/bikeshare_raw/201508_station_data.csv > %s/bikeshare_raw/station_data_3.csv' % (self.DATA_DIR, self.DATA_DIR), shell=True)
        subprocess.call('cat %s/bikeshare_raw/station_data_1.csv %s/bikeshare_raw/station_data_2.csv %s/bikeshare_raw/station_data_3.csv > %s/bikeshare_raw/station_data.csv' % (self.DATA_DIR, self.DATA_DIR, self.DATA_DIR, self.DATA_DIR), shell=True)

        # process status data   (works locally)
        subprocess.call('tail -n +2 %s/bikeshare_raw/201402_status_data.csv > %s/bikeshare_raw/status_data_1.csv' % (self.DATA_DIR, self.DATA_DIR), shell=True)
        subprocess.call('tail -n +2 %s/bikeshare_raw/201408_status_data.csv > %s/bikeshare_raw/status_data_2.csv' % (self.DATA_DIR, self.DATA_DIR), shell=True)
        subprocess.call('tail -n +2 %s/bikeshare_raw/201508_status_data.csv > %s/bikeshare_raw/status_data_3.csv' % (self.DATA_DIR, self.DATA_DIR), shell=True)
        subprocess.call('cat %s/bikeshare_raw/status_data_1.csv %s/bikeshare_raw/status_data_2.csv %s/bikeshare_raw/status_data_3.csv > %s/bikeshare_raw/status_data.csv' % (self.DATA_DIR, self.DATA_DIR, self.DATA_DIR, self.DATA_DIR), shell=True)

        # create directories in HDFS
        subprocess.call(
            '%s/hadoop/hadoop-hdfs.sh '
            'dfs -mkdir hdfs://hadoop:9000/station_data' % self.ROOT_DIR,
            shell=True)
        subprocess.call(
            '%s/hadoop/hadoop-hdfs.sh '
            'dfs -mkdir hdfs://hadoop:9000/status_data' % self.ROOT_DIR,
            shell=True)

        # copy processed station and status data to directories in HDFS
        subprocess.call(
            '%s/hadoop/hadoop-hdfs.sh '
            'dfs -put /root/data/bikeshare_raw/station_data.csv '
            'hdfs://hadoop:9000/station_data' % self.ROOT_DIR,
            shell=True)
        subprocess.call(
            '%s/hadoop/hadoop-hdfs.sh '
            'dfs -put /root/data/bikeshare_raw/status_data.csv '
            'hdfs://hadoop:9000/status_data' % self.ROOT_DIR,
            shell=True)

        print "Running transform in Spark..."

        # Run transform in Spark
        subprocess.call(
            '%s/spark/run-pyspark-cmd.sh '
            '/root/etl/bikeshare/spark-bikeshare-transform.py' % self.ROOT_DIR,
            shell=True
        )

if __name__ == '__main__':

    DATA_DIR = '../../data' # testing locally
    importer = BikeshareImport(DATA_DIR)
    importer.do_import()
