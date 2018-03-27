#!/usr/bin/env python2.7
from __future__ import print_function
import logging
import sys
import os
import mysql.connector
from mysql.connector import errorcode
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

class FetchBlob:
    def __init__(self, arguments):
        self._hostname  = arguments.hostname
        self._database  = arguments.database
        self._username  = arguments.username
        self._password  = arguments.password
        self._table     = arguments.table
        self._uniqueid  = arguments.uniqueid
        self._blobfield = arguments.blobfield
        self._outdir    = arguments.out
        self._suffix    = arguments.suffix
        logging.debug("Instance of Options initialized with: %s" % 
                self.__dict__)

    def connect_database(self):
        try:
            self._conn = mysql.connector.connect(host=self._hostname,
                    user=self._username,
                    password=self._password,
                    database=self._database )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                print(err)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                print(err)
            else:
                print(err)
        else:
            self._cursor = self._conn.cursor()

    def get_cursor(self):
        self._cursor = self._conn.cursor()


    def fetch_blobs(self):
        self.connect_database()
        sql = 'select %s, %s from %s;' % (self._uniqueid, self._blobfield, 
                self._table)
        self._cursor.execute(sql)
        for (idx, blobfield) in self._cursor:
            process_blobs(self._outdir, idx,
                    self._suffix, blobfield)


    def destroy(self):
        self._cursor.close()
        self._conn.close()

def process_blobs(DIRECTORY,ID, SUFFIX, CONTENT):
    filename = "%s%s.%s" % (DIRECTORY, ID, SUFFIX)
    f = open(filename, 'w+')
    f.write(CONTENT);
    f.close()
    return filename


if __name__ == '__main__':
    print("This only executes when %s is executed rather than imported" 
            % __file__)
    import argparse

    parser = argparse.ArgumentParser(description='fetch blobs from mysql' + 
            ' database and apply some operations to them.')
    parser.add_argument('-H','--hostname', type=str, nargs='?',
                    default='localhost', help='')
    parser.add_argument('-d','--database', type=str, nargs='?',
            required=True)
    parser.add_argument('-u','--username', type=str, nargs='?',
            required=True)
    parser.add_argument('-p','--password', type=str, nargs='?',
            required=True)
    parser.add_argument('-t','--table', type=str, nargs='?',
            required=True)
    parser.add_argument('-i','--uniqueid', type=str, nargs='?',
            required=True, help='usually the primary key')
    parser.add_argument('-b','--blobfield', type=str, nargs='?',
            required=True, help='list of fields containing binary blobs ' +
            'files will be prefixed with table name')
    parser.add_argument('-o','--out', type=str, nargs='?',
            required=True, help='output directory')
    parser.add_argument('-s','--suffix', type=str, nargs='?',
            required=True, help='file suffix')

    args = parser.parse_args()
    fetch = FetchBlob(args)
    fetch.connect_database()
    fetch.get_cursor()
    fetch.fetch_blobs()
    fetch.destroy()    

