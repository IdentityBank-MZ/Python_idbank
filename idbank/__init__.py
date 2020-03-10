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

from .idbankcommon import IdbCommon, IdbConfig
from .idbankstorage import IdbankStorageFormat, IdbankStorageType, IdbankStorageTags, IdbankStorageEngine, \
    IdbankStorageBase, AwsDynamoDb
from .idbankquery import IdbQueryResponse, IdbQueryBusiness, IdbQueryPeople, IdbQueryRelation, IdbQuery, \
    IdbSqlQueryBuilder, IdbQueryError
from .idbankhelper import ProcessQuery, IdbServer


################################################################################
# Module                                                                       #
################################################################################

def idbQuery(jscConfigFilePath, connectionName, query=None):
    return ProcessQuery.execute(jscConfigFilePath, connectionName, query)


__all__ = ('IdbCommon', 'IdbConfig',
           'IdbQueryResponse', 'IdbQueryBusiness', 'IdbQueryPeople', 'IdbQueryRelation', 'IdbQuery',
           'IdbSqlQueryBuilder',
           'IdbQueryError',
           'IdbankStorageFormat', 'IdbankStorageType', 'IdbankStorageTags', 'IdbankStorageEngine', 'IdbankStorageBase',
           'AwsDynamoDb',
           'ProcessQuery', 'IdbServer')

################################################################################
#                                End of file                                   #
################################################################################
