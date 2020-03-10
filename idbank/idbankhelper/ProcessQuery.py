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

import logging
import json

from idbank import IdbConfig
from idbank import IdbQuery, IdbQueryError


################################################################################
# Module                                                                       #
################################################################################

class ProcessQuery:

    @staticmethod
    def executeFromFile(jscConfigFilePath: str,
                        connectionName: str,
                        inputQueryFilePath: str):

        if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
            with open(inputQueryFilePath, 'r') as queryFile:
                query = queryFile.read()
        else:
            try:
                with open(inputQueryFilePath, 'r') as queryFile:
                    query = queryFile.read()
            except:
                query = None
                logging.error("There is problem with your query. Check it and try again.")

        if query:
            queryJsonData = json.loads(query)
            returnValue = IdbQueryError.requestError()

            if connectionName:
                connectionName = connectionName.strip('"').strip("'")
                configuration = IdbConfig.getConfig(jscConfigFilePath, connectionName)

                if configuration:
                    if isinstance(queryJsonData, dict):
                        returnValue = IdbQuery.execute(configuration, query)
                    else:
                        returnValue = []
                        for query in queryJsonData:
                            returnValue.append(IdbQuery.execute(configuration, json.dumps(query)))

            return returnValue

    @staticmethod
    def execute(jscConfigFilePath: str,
                connectionName: str,
                query: str) -> str:

        returnValue = IdbQueryError.requestError()

        if connectionName:
            connectionName = connectionName.strip('"').strip("'")
            configuration = IdbConfig.getConfig(jscConfigFilePath, connectionName)
            if configuration and query:
                returnValue = IdbQuery.execute(configuration, query)

        return returnValue

################################################################################
#                                End of file                                   #
################################################################################
