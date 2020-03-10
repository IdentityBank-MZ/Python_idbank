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

import decimal
import logging
import json

from .IdbQueryError import IdbQueryError
from .IdbQueryBusiness import IdbQueryBusiness
from .IdbQueryPeople import IdbQueryPeople
from .IdbQueryRelation import IdbQueryRelation


################################################################################
# Module                                                                       #
################################################################################

class IdbQuery:

    @staticmethod
    def execute(configuration: dict,
                query: str) -> str:

        returnValue = IdbQueryError.requestUnsupported()

        try:
            queryData = json.loads(query, parse_float=decimal.Decimal)
        except json.decoder.JSONDecodeError as error:
            logging.error('Error decoding query data: ' + str(error))
            queryData = None

        if configuration['connectionType'] == 'IdentityBank.V1':
            returnValue = IdbQuery.executeQuery(configuration, queryData)
        else:
            print("The IDB connection type '{}' is not supported.".format(configuration['connectionType']))

        return returnValue

    @staticmethod
    def executeQuery(configuration: dict,
                     queryData: dict) -> str:

        returnValue = IdbQueryError.requestError()

        try:
            if isinstance(queryData, dict) and 'service' in queryData:
                if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
                    logging.debug("IDB query data: " + json.dumps(queryData))

                if queryData['service']:
                    if queryData['service'] == 'business':
                        returnValue = IdbQueryBusiness.executeQuery(configuration, queryData)
                    if queryData['service'] == 'people':
                        returnValue = IdbQueryPeople.executeQuery(configuration, queryData)
                    if queryData['service'] == 'relation':
                        returnValue = IdbQueryRelation.executeQuery(configuration, queryData)

        except Exception as e:
            returnValue = IdbQueryError.requestUnsupportedService()
            logging.error('Query error')
            logging.error(str(e))

        return returnValue

################################################################################
#                                End of file                                   #
################################################################################
