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

import psycopg2
from psycopg2 import DatabaseError

from .IdbQueryError import IdbQueryError
from .IdbQueryResponse import IdbQueryResponse


################################################################################
# Module                                                                       #
################################################################################

class IdbQuerySql:

    @staticmethod
    def querySqlToString(query: dict, cursor) -> str:
        sqlString = ''
        try:
            if cursor and query and 'sql' in query:
                if isinstance(query['sql'], list):
                    for item in query['sql']:
                        sqlString += item.as_string(cursor)
                else:
                    sqlString = query['sql'].as_string(cursor)
        except (Exception, DatabaseError) as e:
            pass
        return sqlString

    @staticmethod
    def queryToString(query: dict, cursor) -> str:
        queryString = {}
        try:
            if cursor and query and 'sql' in query:
                queryString['sql'] = IdbQuerySql.querySqlToString(query, cursor)
            if query and 'data' in query:
                queryString['data'] = {}
                for key, value in query['data'].items():
                    try:
                        json.dumps(value)
                        queryString['data'][key] = value
                    except (Exception, TypeError, OverflowError):
                        pass
        except (Exception, DatabaseError) as e:
            pass
        return json.dumps(queryString)

    @staticmethod
    def logging(dbConnection: dict, query: dict, cursor=None) -> str:
        if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
            logging.debug(
                "DB Connection: " + json.dumps({key: dbConnection[key] for key in dbConnection if key != 'password'}))
            logging.debug("Execute query: " + IdbQuerySql.queryToString(query, cursor))

    @staticmethod
    def executeSqlQuery(dbConnection: dict, query: dict,
                        returnStatusOnly: bool = False):
        try:
            logging.debug("IDB execute Sql query")
            connection = psycopg2.connect(**dbConnection)
            cursor = connection.cursor()
            IdbQuerySql.logging(dbConnection, query, cursor)
            cursor.execute(IdbQuerySql.querySqlToString(query, cursor), query['data'])
            connection.commit()
            if returnStatusOnly:
                returnValue = {"Query": cursor.rowcount}
            else:
                returnValue = IdbQueryResponse.responseOkDict({"Query": cursor.rowcount})
        except (Exception, DatabaseError) as error:
            if returnStatusOnly:
                returnValue = {"Query": cursor.rowcount, "QueryError": str(error)}
            else:
                returnValue = IdbQueryError.requestQueryError(str(error))
            logging.error('Query error')
            logging.error(str(error))
        finally:
            if (connection):
                cursor.close()
                connection.close()
        return returnValue

    @staticmethod
    def fetchSqlQuery(dbConnection: dict, query: dict, commit: bool = False,
                      returnDataOnly: bool = False):
        try:
            logging.debug("IDB fetch Sql query")
            connection = psycopg2.connect(**dbConnection)
            cursor = connection.cursor()
            IdbQuerySql.logging(dbConnection, query, cursor)
            cursor.execute(IdbQuerySql.querySqlToString(query, cursor), query['data'])
            if commit:
                connection.commit()
            if returnDataOnly:
                returnValue = {"Query": cursor.rowcount, "QueryData": cursor.fetchall()}
            else:
                returnValue = IdbQueryResponse.responseOkDict(
                    {"Query": cursor.rowcount, "QueryData": cursor.fetchall()})
        except (Exception, DatabaseError) as error:
            returnValue = IdbQueryError.requestQueryError(str(error))
            logging.error('Query error')
            logging.error(str(error))
        finally:
            if (connection):
                cursor.close()
                connection.close()
        return returnValue

################################################################################
#                                End of file                                   #
################################################################################
