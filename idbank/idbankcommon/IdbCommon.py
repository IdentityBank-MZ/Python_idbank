# -*- coding: utf-8 -*-
# * ********************************************************************* *
# *                                                                       *
# *   Identity Bank data driver                                           *
# *   This file is part of idbank. This project may be found at:          *
# *   https://github.com/IdentityBank/Python_idbank.                      *
# *                                                                       *
# *   Copyright (C) 2020 by Identity Bank. All Rights Reserved.           *
# *   https://www.identitybank.eu - You belong to you                     *
# *                                                                       *
# *   This program is free software: you can redistribute it and/or       *
# *   modify it under the terms of the GNU Affero General Public          *
# *   License as published by the Free Software Foundation, either        *
# *   version 3 of the License, or (at your option) any later version.    *
# *                                                                       *
# *   This program is distributed in the hope that it will be useful,     *
# *   but WITHOUT ANY WARRANTY; without even the implied warranty of      *
# *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the        *
# *   GNU Affero General Public License for more details.                 *
# *                                                                       *
# *   You should have received a copy of the GNU Affero General Public    *
# *   License along with this program. If not, see                        *
# *   https://www.gnu.org/licenses/.                                      *
# *                                                                       *
# * ********************************************************************* *

################################################################################
# Import(s)                                                                    #
################################################################################

import os
import sys
import datetime
import hashlib
import string
import codecs
import unicodedata
import collections


################################################################################
# Module                                                                       #
################################################################################

class IdbCommon:

    @staticmethod
    def getTimestemp(time=True, seconds=False, microseconds=False, utc=False):
        if utc:
            todaydate = datetime.datetime.utcnow()
        else:
            todaydate = datetime.datetime.now()
        timestemp = (str("%04d" % todaydate.year) + "-" +
                     str("%02d" % todaydate.month) + "-" +
                     str("%02d" % todaydate.day))
        if time:
            timestemp += (" " +
                          str("%02d" % todaydate.hour) + ":" +
                          str("%02d" % todaydate.minute))
            if seconds:
                timestemp += ("." + str("%02d" % todaydate.second))
                if microseconds:
                    timestemp += ("." + str("%06d" % todaydate.microsecond))
        return timestemp

    @staticmethod
    def getSimpleTimestemp(time=True, seconds=True, microseconds=True, utc=False):
        if utc:
            todaydate = datetime.datetime.utcnow()
        else:
            todaydate = datetime.datetime.now()
        timestemp = (str("%04d" % todaydate.year) +
                     str("%02d" % todaydate.month) +
                     str("%02d" % todaydate.day))
        if time:
            timestemp += (str("%02d" % todaydate.hour) +
                          str("%02d" % todaydate.minute))
            if seconds:
                timestemp += (str("%02d" % todaydate.second))
                if microseconds:
                    timestemp += (str("%06d" % todaydate.microsecond))
        return timestemp

    @staticmethod
    def md5sum(filename, seek=0, blocksize=4096):
        hash = hashlib.md5()
        with open(filename, "rb") as file:
            if seek > 0:
                file.seek(seek)
            for block in iter(lambda: file.read(blocksize), b""):
                hash.update(block)
        return hash.hexdigest()

    @staticmethod
    def queryYesNo(question, default="yes"):

        valid = {"yes": True, "y": True, "True": True, "T": True, "t": True, '1': True,
                 "no": False, "n": False, "False": False, "F": False, "f": False, '0': False}
        if default is None:
            prompt = " [y/n] "
        elif default == "yes":
            prompt = " [Y/n] "
        elif default == "no":
            prompt = " [y/N] "
        else:
            raise ValueError("invalid default answer: '%s'" % default)

        while True:
            sys.stdout.write(question + prompt)
            choice = input().lower()
            if default is not None and choice == '':
                return valid[default]
            elif choice in valid:
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'yes/y/t/1' or 'no/n/f/0'" + os.linesep)

    @staticmethod
    def slugify(value):
        validFilenameChars = "-_.() %s%s" % (string.ascii_letters, string.digits)
        value = codecs.decode(unicodedata.normalize('NFKD', value).encode('ascii', 'ignore'), 'ascii')
        return ''.join(str(char) for char in value if str(char) in validFilenameChars)

    @staticmethod
    def str2Bool(value):
        if value.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif value.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise TypeError('Boolean value expected.')

    @staticmethod
    def dictionaryMerge(dictionary, mergeDictionary):
        for k, v in mergeDictionary.items():
            if (k in dictionary and isinstance(dictionary[k], dict)
                    and isinstance(mergeDictionary[k], collections.Mapping)):
                IdbCommon.dictionaryMerge(dictionary[k], mergeDictionary[k])
            else:
                dictionary[k] = mergeDictionary[k]
        return dictionary

################################################################################
#                                End of file                                   #
################################################################################
