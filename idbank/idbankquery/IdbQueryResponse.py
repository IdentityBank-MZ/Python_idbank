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

import hashlib
import json
import datetime

from idbank import IdbCommon


################################################################################
# Module                                                                       #
################################################################################

class IdbQueryResponse:

    @staticmethod
    def customJsonDumpDefault(o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    @staticmethod
    def response(statusCode: int,
                 statusMessage: str = None,
                 responseData: str = None,
                 requestId: str = None,
                 contentType: str = 'json') -> str:
        timestamp = IdbCommon.getSimpleTimestemp()
        if responseData:
            contentLength = len(responseData)
            checksum = hashlib.md5(responseData.encode('UTF-8')).hexdigest()
        else:
            contentLength = 0
            checksum = None

        returnData = {
            'requestId': requestId,
            'statusCode': statusCode,
            'statusMessage': statusMessage,
            'timestamp': timestamp,
            'result': responseData,
            'contentType': contentType,
            'contentLength': contentLength,
            'checksum': checksum
        }
        return json.dumps(returnData, ensure_ascii=False)

    @staticmethod
    def responseOk(responseData: str = None,
                   requestId: str = None) -> str:
        return IdbQueryResponse.response(200,
                                         'OK',
                                         responseData,
                                         requestId)

    @staticmethod
    def responseOkDict(responseData: dict,
                       requestId: str = None) -> str:
        return IdbQueryResponse.responseOk(
            json.dumps(responseData, default=IdbQueryResponse.customJsonDumpDefault),
            requestId)

    @staticmethod
    def responseCreated(responseData: str = None,
                        requestId: str = None) -> str:
        return IdbQueryResponse.response(201,
                                         'Created',
                                         responseData,
                                         requestId)

    @staticmethod
    def responseCreatedDict(responseData: dict,
                            requestId: str = None) -> str:
        return IdbQueryResponse.responseCreated(
            json.dumps(responseData, default=IdbQueryResponse.customJsonDumpDefault),
            requestId)

################################################################################
#                                End of file                                   #
################################################################################
