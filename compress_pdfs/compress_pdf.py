#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import sys
import os
import re
from multiprocessing import Pool
from tempfile import mkstemp
import logging
logging.basicConfig(stream=sys.stderr, level=logging.ERROR)

class CompressPdf:
    def __init__(self,arguments):
        self._command = ("gs -q -dNOPAUSE -dBATCH -sDEVICE=pdfwrite" +
        " -dCompatibilityLevel=1.4 -dPDFSETTINGS=/printer -sOutputFile=")
        if arguments.directory:
    #        print (arguments.directory)
            self._directory = arguments.directory
            print ('%s' % self._directory)
            self.walk_dir()
        else:
            print ('%s' % arguments.file)
            self.compress_file(arguments.file)
    
    def walk_dir(self):
        # we get ourselfs a multiprocessing pool
        pool = Pool(processes=4)
        files_with_abs_path = ['']
        for root, dirs, files in os.walk(self._directory):
            path = root.split(os.sep)
            for file in files:
                files_with_abs_path.append((os.path.join(root, file)))
            results = pool.map(self.compress_file, files_with_abs_path)


    def compress_file(self,filename_with path):
#   compress file in memory using mkstemp() and 
        fd, temp_path = mkstemp()
        command = ("" + self._command + temp_path + ' ' + filename_with_path)
        os.system(command)
        command = ('mv ' + temp_path + ' ' + filename_with_path)
        os.system(command)
        # usually we should tidy up behind ourself and call 
        # os.remove(temp_path) but since we just moved the
        # file we don't need to

    def destroy(self):
        sys.exit(0)

if __name__ == '__main__':
    print("This only executes when %s is executed rather than imported" 
            % __file__)
    parser = argparse.ArgumentParser(description='This program takes a pdf' +
            ' file or directory containing pdfs and uses ghostscript to' +
            'compress them. ATTENTION files will be overwritten')
    parser.add_argument('-v','--verbose', type=str, nargs='?',
            required=False)
    parser.add_argument('-d','--directory', type=str, nargs='?',
            required=False, help='compress all files in dir recursively')
    parser.add_argument('-f','--file', type=str, nargs='?',
            required=False, help='compress single file')
    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error('No arguments provided.')
    args = parser.parse_args()
    comp = CompressPdf(args)



