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

from psycopg2 import sql

from .IdbQueryError import IdbQueryError
from .IdbQueryResponse import IdbQueryResponse
from .IdbQuerySql import IdbQuerySql
from .IdbSqlQueryBuilder import IdbSqlQueryBuilder


################################################################################
# Module                                                                       #
################################################################################

class IdbQueryRelation:

    @staticmethod
    def executeQuery(configuration: dict,
                     queryData: dict) -> str:

        logging.debug("IDB - execute query ")
        returnValue = IdbQueryError.requestError()

        try:
            if isinstance(queryData, dict) and 'query' in queryData and configuration['connectionRelation']:

                queryData['dbTableSchema'] = 'p57b_relations'
                queryData['dbTableName'] = 'business2people'
                queryData['dbTableSchemaIdentifier'] = sql.Identifier(queryData['dbTableSchema'])
                queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
                queryData['bidIdentifier'] = sql.Identifier('bid')
                queryData['pidIdentifier'] = sql.Identifier('pid')
                if 'businessId' in queryData and \
                        queryData['businessId'] and \
                        not ('peopleId' in queryData and
                             queryData['peopleId']):
                    queryData['businessIdPlaceholder'] = sql.Placeholder('businessId')
                    queryData['dbTableCondition'] = sql.SQL("""
                    WHERE
                    {bidIdentifier} LIKE {businessIdPlaceholder}""").format(**queryData)
                elif 'peopleId' in queryData and \
                        queryData['peopleId'] and \
                        not ('businessId' in queryData and
                             queryData['businessId']):
                    queryData['peopleIdPlaceholder'] = sql.Placeholder('peopleId')
                    queryData['dbTableCondition'] = sql.SQL("""
                    WHERE
                    {pidIdentifier} LIKE {peopleIdPlaceholder}""").format(**queryData)
                else:
                    queryData['businessIdPlaceholder'] = sql.Placeholder('businessId')
                    queryData['peopleIdPlaceholder'] = sql.Placeholder('peopleId')
                    queryData['dbTableCondition'] = sql.SQL("""
                    WHERE
                    {bidIdentifier} LIKE {businessIdPlaceholder}
                    AND
                    {pidIdentifier} LIKE {peopleIdPlaceholder}""").format(**queryData)
                dbConnection = {
                    "host": configuration['connectionRelation']['dbHost'],
                    "port": configuration['connectionRelation']['dbPort'],
                    "database": configuration['connectionRelation']['dbName'],
                    "user": configuration['connectionRelation']['dbUser'],
                    "password": configuration['connectionRelation']['dbPassword'],
                }
                if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
                    dbConnectionPrint = dbConnection.copy()
                    dbConnectionPrint.pop("password")
                    logging.debug(json.dumps(dbConnectionPrint))

                logging.debug("IDB query execute: " + json.dumps(queryData['query']))

                if queryData['query'] == 'setRelationBusiness2People':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlSetRelationBusiness2People(
                                                                  queryData))
                elif queryData['query'] == 'deleteRelationBusiness2People':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlDeleteRelationBusiness2People(
                                                                  queryData))
                elif queryData['query'] == 'checkRelationBusiness2People':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlCheckRelationBusiness2People(
                                                                queryData))
                elif queryData['query'] == 'checkRelationsBusiness2People':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlCheckRelationsBusiness2People(
                                                                queryData))
                elif queryData['query'] == 'getRelatedPeoples':
                    queryData['dbTableCondition'] = sql.SQL(
                        'WHERE {bidIdentifier} LIKE {businessIdPlaceholder}').format(**queryData)
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlGetRelatedPeoples(
                                                                queryData))
                elif queryData['query'] == 'getRelatedBusinesses':
                    queryData['dbTableCondition'] = sql.SQL('WHERE {pidIdentifier} LIKE {peopleIdPlaceholder}').format(
                        **queryData)
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlGetRelatedBusinesses(
                                                                queryData))
                elif queryData['query'] == 'getRelatedPeoplesCountAll':
                    queryData['dbTableCondition'] = sql.SQL(
                        'WHERE {bidIdentifier} LIKE {businessIdPlaceholder}').format(**queryData)
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlCountAllItems(
                                                                dict(queryData)), False, True)
                    if returnValue \
                            and isinstance(returnValue, dict) \
                            and 'Query' in returnValue \
                            and returnValue['Query'] == 1 \
                            and 'QueryData' in returnValue:
                        countAll = returnValue['QueryData']
                        returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                                IdbSqlQueryBuilder.generateSqlGetRelatedPeoples(
                                                                    queryData), False, True)
                        if returnValue \
                                and isinstance(returnValue, dict) \
                                and 'Query' in returnValue \
                                and 'QueryData' in returnValue:
                            returnValue['CountAll'] = countAll
                            returnValue = IdbQueryResponse.responseOkDict(returnValue)
                        else:
                            returnValue = IdbQueryError.requestQueryError()
                    else:
                        returnValue = IdbQueryError.requestQueryError()
                elif queryData['query'] == 'getRelatedBusinessesCountAll':
                    queryData['dbTableCondition'] = sql.SQL('WHERE {pidIdentifier} LIKE {peopleIdPlaceholder}').format(
                        **queryData)
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlCountAllItems(
                                                                dict(queryData)), False, True)
                    if returnValue \
                            and isinstance(returnValue, dict) \
                            and 'Query' in returnValue \
                            and returnValue['Query'] == 1 \
                            and 'QueryData' in returnValue:
                        countAll = returnValue['QueryData']
                        returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                                IdbSqlQueryBuilder.generateSqlGetRelatedBusinesses(
                                                                    queryData), False, True)
                        if returnValue \
                                and isinstance(returnValue, dict) \
                                and 'Query' in returnValue \
                                and 'QueryData' in returnValue:
                            returnValue['CountAll'] = countAll
                            returnValue = IdbQueryResponse.responseOkDict(returnValue)
                        else:
                            returnValue = IdbQueryError.requestQueryError()
                    else:
                        returnValue = IdbQueryError.requestQueryError()
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
