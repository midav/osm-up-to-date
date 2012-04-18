#!/usr/bin/env python

import urllib, os, sys, subprocess, ConfigParser, logging
import time, datetime

osm_url = "http://planet.openstreetmap.org/daily/timestamp.txt"
logger = logging.getLogger('osm-up-to-date-planet')
hdlr = logging.FileHandler('osm-up-to-date-planet.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

def get_date():
    times_tamp = urllib.urlopen( osm_url )
    osm_date = times_tamp.read()[:10]
    return osm_date

def download_lock():
    with open('download.lock', 'r') as f:
        for line in f:
            date = line.split('-')
            date = datetime.date(int(date[0]), int(date[1]), int(date[2])) 
            delta = date - datetime.date.today()
            global difference
            difference = date + datetime.timedelta(days=1)
            if delta <= datetime.timedelta(-1):
                diff_file = date.strftime("%Y%m%d") + "-" + difference.strftime("%Y%m%d") +  ".osc.gz"
                return diff_file

def download_osm():
    get_diff = "http://planet.openstreetmap.org/daily/" + download_lock()
    wget = "wget"
    path = "-P"
    subprocess.call([wget, "-nv", "-a", "osm-up-to-date-planet.log", path, "downloads", get_diff])

def uptodate_lock():
    lock_file = open('download.lock', 'w')
    lock_file.write(str(difference))
    lock_file.close()

def readConfig(file="config.ini"):
        credentials = []
        Config = ConfigParser.ConfigParser()
        Config.read(file)
        postgres = Config.items("POSTGRES")
        for value in postgres:
            credentials.append(value[1])
        return credentials

def remove_difffile():
    remove = "rm"
    subprocess.call([remove, "downloads" + "/" + download_lock()])

def run_populating():
    user, db, pas = readConfig()
    osmosis = "osmosis"
    run_osmosis = subprocess.Popen([osmosis, "--read-xml-change-0.6", "compressionMethod=gzip", "downloads" + "/" + download_lock(),
        "--write-apidb-change-0.6", "user=" + user, "database=" + db,
        "password=" + "\"" + pas + "\"", "validateSchemaVersion=no" ], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    if run_osmosis.wait() != 0:
        logger.info('Populating failed due to issue with osmosis, see the log file')
        output = run_osmosis.communicate()[0]
        logger.info(output)
        remove_difffile()
        sys.exit(1)
    else:
        remove_difffile()
        logger.info('Changes were successfully applied the database')

def main():
       download_lock()
       logger.info('Startin process of populatin')
       while str(difference) <= get_date():
           download_osm()
           run_populating()
           uptodate_lock()
           download_lock()
       else:
           logger.info('Planet is up to date')
           sys.exit(0)
if __name__ == '__main__':
        main()
