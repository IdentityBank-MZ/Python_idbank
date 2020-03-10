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

import bintexttools

from .IdbankStorageFormat import IdbankStorageTags, IdbankStorageFormat, IdbankStorageType, IdbankStorageInfo


################################################################################
# Module                                                                       #
################################################################################

class IdbankStorageEngine:
    __detectFormat = True
    __format = IdbankStorageFormat.default
    __type = IdbankStorageType.default

    def __init__(self, options: dict = None):
        if options and 'idbFormat' in options \
                and isinstance(options['idbFormat'], str):
            self.__format = options['idbFormat']

        if options and 'idbType' in options \
                and isinstance(options['idbType'], str):
            self.__type = options['idbType']

        self.__options = options

    def convert(self, data: str):
        if self.__format == IdbankStorageFormat.IDB_FORMAT_CLEAR:
            return self.__convertClear(data)
        elif self.__format == IdbankStorageFormat.IDB_FORMAT_V1:
            return self.__convertIdbV1(data)
        return self.__convertClear(data)

    def __convertClear(self, data: str):
        return data

    def __convertIdbV1(self, data: str):
        dataText = None
        if data:
            bytearrayData = bytearray(data, 'utf-8')
            bin2text = bintexttools.Bin2TextConverter()
            bin2text.format = IdbankStorageType.toBinTextTools(self.__type)
            dataBin2Text = bin2text.convert(bytearrayData)
            if dataBin2Text:
                dataTextPrefix = IdbankStorageInfo.formatter(IdbankStorageTags.IDB_FORMAT_TAG,
                                                             self.__format,
                                                             first=True)
                dataTextPrefix += IdbankStorageInfo.formatter(IdbankStorageTags.IDB_TYPE_TAG,
                                                              self.__type)
                dataText = dataTextPrefix + IdbankStorageInfo.formatter(IdbankStorageTags.IDB_DATA_TAG,
                                                                        dataBin2Text)
        return dataText

    def parse(self, data: str):
        dataBytes = None
        if data:
            format = IdbankStorageFormat.detect(data)
            if format:
                if format['value'] == IdbankStorageFormat.IDB_FORMAT_CLEAR:
                    dataBytes = data
                elif format['value'] == IdbankStorageFormat.IDB_FORMAT_V1:
                    dataBytes = self.__parseIdbV1(format['data'])
            else:
                # Assuming clear data
                dataBytes = data

        return dataBytes

    def __parseIdbV1(self, data: str):
        dataBytes = None
        if data:
            format = IdbankStorageType.detect(data)
            text2bin = bintexttools.Text2BinConverter()
            text2bin.format = IdbankStorageType.toBinTextTools(format['value'])

            formatData = IdbankStorageInfo.detectTagValue(format['data'], IdbankStorageTags.IDB_DATA_TAG)
            if formatData:
                data = text2bin.convert(formatData['value'])
                if data:
                    dataBytes = data.decode("utf-8")
        return dataBytes

################################################################################
#                                End of file                                   #
################################################################################
