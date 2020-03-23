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

import json
import logging

from .IdbQueryError import IdbQueryError
from .IdbQueryResponse import IdbQueryResponse
from idbank import AwsDynamoDb


################################################################################
# Module                                                                       #
################################################################################

class IdbQueryPeople:

    @staticmethod
    def executeQuery(configuration: dict,
                     queryData: dict) -> str:

        logging.debug("IDB - execute query ")
        returnValue = IdbQueryError.requestError()

        try:
            if isinstance(queryData, dict) and 'query' in queryData:

                logging.debug("IDB query execute: " + json.dumps(queryData['query']))

                if queryData['query'] == 'countAllItems' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        returnValue = str(idb.countAllItems())
                        if returnValue:
                            returnValue = IdbQueryResponse.responseOkDict({"count": returnValue})
                        else:
                            returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'putItem' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        idbId = None
                        if 'idbId' in queryData:
                            idbId = queryData['idbId']
                            data = queryData['data']
                            returnValue = str(idb.putItem(idbId, data))
                            if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                                returnValue = IdbQueryResponse.responseCreatedDict({"Created": 1})
                            elif returnValue and returnValue == 'The conditional request failed':
                                returnValue = IdbQueryError.itemAlreadyExisting()
                            else:
                                returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'getItem' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        idbId = None
                        if 'idbId' in queryData:
                            idbId = queryData['idbId']
                            returnValue = str(idb.getItem(idbId))
                            if returnValue:
                                if returnValue == 'None':
                                    returnValue = IdbQueryError.itemNotFound()
                                else:
                                    returnValue = IdbQueryResponse.responseOkDict({"Data": returnValue})
                            else:
                                returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'updateItem' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        idbId = None
                        if 'idbId' in queryData:
                            idbId = queryData['idbId']
                            data = queryData['data']
                            returnValue = str(idb.updateItem(idbId, data))
                            if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                                returnValue = IdbQueryResponse.responseOkDict({"Updated": 1})
                            elif returnValue and returnValue == 'The conditional request failed':
                                returnValue = IdbQueryError.itemNotFound()
                            else:
                                returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'deleteItem' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        idbId = None
                        if 'idbId' in queryData:
                            idbId = queryData['idbId']
                            returnValue = str(idb.deleteItem(idbId))
                            if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                                returnValue = IdbQueryResponse.responseOkDict({"Deleted": 1})
                            elif returnValue and returnValue == 'The conditional request failed':
                                returnValue = IdbQueryError.itemNotFound()
                        else:
                            returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'createAccountMetadata' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        returnValue = str(idb.createAccountMetadata())
                        if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                            returnValue = IdbQueryResponse.responseCreatedDict({"Metadata Created": 1})
                        elif returnValue and returnValue == 'The conditional request failed':
                            returnValue = IdbQueryError.itemAlreadyExisting()
                        else:
                            returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'setAccountMetadata' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        idbId = None
                        if 'metadata' in queryData:
                            metadata = queryData['metadata']
                            returnValue = str(idb.setAccountMetadata(metadata))
                            if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                                returnValue = IdbQueryResponse.responseOkDict({"Metadata updated": 1})
                            elif returnValue and returnValue == 'The conditional request failed':
                                returnValue = IdbQueryError.itemNotFound()
                            else:
                                returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'getAccountMetadata' and configuration['connectionPeople']:
                    idb = AwsDynamoDb.idb(configuration['connectionPeople'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        returnValue = str(idb.getAccountMetadata())
                        if returnValue:
                            if returnValue == 'None':
                                returnValue = IdbQueryError.itemNotFound()
                            else:
                                returnValue = IdbQueryResponse.responseOkDict({"Metadata": returnValue})
                        else:
                            returnValue = IdbQueryError.requestInternalServerError()
                else:
                    returnValue = IdbQueryError.requestNotImplemented()

        except Exception as e:
            returnValue = IdbQueryError.requestUnsupportedService(str(e))
            logging.error('Query error')
            logging.error(str(e))

        logging.debug("IDB execution done: " + json.dumps(queryData['query']))

        return returnValue

################################################################################
#                                End of file                                   #
################################################################################
