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


################################################################################
# Module                                                                       #
################################################################################

class IdbankStorageInfo:
    IDB_SEPARATOR = "."

    @staticmethod
    def formatter(tag: str, tagData: str, first: bool = False):
        if first:
            firstTag = ""
        else:
            firstTag = IdbankStorageInfo.IDB_SEPARATOR

        if tag and tagData:
            return "{}{}{}{}".format(firstTag,
                                     tag,
                                     IdbankStorageInfo.IDB_SEPARATOR,
                                     tagData)
        return ""

    @staticmethod
    def detectTagValue(data: str, tag: str):
        formatString = None
        formatTag = data[:len(tag)]
        if formatTag == tag:
            data = data[len(formatTag) + len(IdbankStorageInfo.IDB_SEPARATOR):]
            formatValuePos = data.find(IdbankStorageInfo.IDB_SEPARATOR)
            if formatValuePos > 0:
                formatValue = data[:formatValuePos]
                data = data[formatValuePos + 1:]
            else:
                formatValue = data
                data = None
            if formatValue:
                formatString = {
                    'tag': formatTag,
                    'value': formatValue,
                    'data': data
                }
        return formatString


class IdbankStorageTags:
    # Available tags
    IDB_FORMAT_TAG = 'IDB_F'
    IDB_TYPE_TAG = 'IDB_T'
    IDB_COMPRESSION_TAG = 'IDB_C'
    IDB_ENCRYPTION_TAG = 'IDB_E'
    IDB_DATA_TAG = 'IDB_D'


class IdbankStorageFormat:
    # Available formats
    IDB_FORMAT_CLEAR = 'CLEAR'
    IDB_FORMAT_V1 = 'V1'

    # Default format for storage engine
    default = IDB_FORMAT_V1

    @staticmethod
    def detect(data: str):
        formatString = None
        tagValue = IdbankStorageInfo.detectTagValue(data, IdbankStorageTags.IDB_FORMAT_TAG)
        if tagValue and isinstance(tagValue, dict) and \
                'tag' in tagValue and \
                'value' in tagValue and \
                'data' in tagValue:
            formatString = tagValue
        return formatString


class IdbankStorageType:
    # Available types
    IDB_TYPE_BINARY = 'B'
    IDB_TYPE_DECIMAL = 'D'
    IDB_TYPE_HEX = 'X'
    IDB_TYPE_ASCII = 'A'
    IDB_TYPE_BASE64 = 'B64'

    # Default type for storage engine
    default = IDB_TYPE_BASE64

    @staticmethod
    def fromBinTextTools(binTextFormat):
        switcher = {
            bintexttools.TextFormat.BINARY: IdbankStorageType.IDB_TYPE_BINARY,
            bintexttools.TextFormat.DECIMAL: IdbankStorageType.IDB_TYPE_DECIMAL,
            bintexttools.TextFormat.HEX: IdbankStorageType.IDB_TYPE_HEX,
            bintexttools.TextFormat.ASCII: IdbankStorageType.IDB_TYPE_ASCII,
            bintexttools.TextFormat.BASE64: IdbankStorageType.IDB_TYPE_BASE64,
        }
        return switcher.get(binTextFormat, IdbankStorageType.default)

    @staticmethod
    def toBinTextTools(type: str):
        switcher = {
            IdbankStorageType.IDB_TYPE_BINARY: bintexttools.TextFormat.BINARY,
            IdbankStorageType.IDB_TYPE_DECIMAL: bintexttools.TextFormat.DECIMAL,
            IdbankStorageType.IDB_TYPE_HEX: bintexttools.TextFormat.HEX,
            IdbankStorageType.IDB_TYPE_ASCII: bintexttools.TextFormat.ASCII,
            IdbankStorageType.IDB_TYPE_BASE64: bintexttools.TextFormat.BASE64,
        }
        return switcher.get(type, bintexttools.TextFormat.BASE64)

    @staticmethod
    def detect(data: str):
        formatString = None
        tagValue = IdbankStorageInfo.detectTagValue(data, IdbankStorageTags.IDB_TYPE_TAG)
        if tagValue and isinstance(tagValue, dict) and \
                'tag' in tagValue and \
                'value' in tagValue and \
                'data' in tagValue:
            formatString = tagValue
        return formatString


class IdbankStorageCompression:
    # Available compressions
    IDB_COMPRESSION_GZIP = 'GZIP'
    IDB_COMPRESSION_BZ2 = 'BZ2'

    # Default type for storage engine
    default = IDB_COMPRESSION_GZIP


class IdbankStorageEncryption:
    # Available encryptions
    IDB_ENCRYPTION_IDB_1 = 'IDB_1'

    # Default type for storage engine
    default = IDB_ENCRYPTION_IDB_1

################################################################################
#                                End of file                                   #
################################################################################
