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

from idbank import AwsDynamoDb
from .IdbQueryError import IdbQueryError
from .IdbQueryResponse import IdbQueryResponse
from .IdbQuerySql import IdbQuerySql
from .IdbSqlQueryBuilder import IdbSqlQueryBuilder


################################################################################
# Module                                                                       #
################################################################################

class IdbQueryBusiness:

    @staticmethod
    def executeQuery(configuration: dict,
                     queryData: dict) -> str:

        logging.debug("IDB - execute query ")
        returnValue = IdbQueryError.requestError()

        try:
            if isinstance(queryData, dict) and 'query' in queryData and configuration['connectionBusiness']:

                allowDelete = True
                allowMigration = 'connectionBusinessData' in configuration and configuration['connectionBusinessData'] and \
                        'allowMigrationIdbank' in configuration['connectionBusinessData'] and \
                        isinstance(configuration['connectionBusinessData']['allowMigrationIdbank'], str) and \
                        configuration['connectionBusinessData']['allowMigrationIdbank'].upper() == 'IDB_ALLOW_MIGRATION'

                queryData['dbTableSchema'] = 'idb_data'
                queryData['dbTableSchemaIdentifier'] = sql.Identifier(queryData['dbTableSchema'])
                queryData['dbTableName'] = "{businessDbId}"
                if 'dbHost' in configuration['connectionBusiness'] and \
                        'dbPort' in configuration['connectionBusiness'] and \
                        'dbName' in configuration['connectionBusiness'] and \
                        'dbUser' in configuration['connectionBusiness'] and \
                        'dbPassword' in configuration['connectionBusiness']:
                    dbConnection = {
                        "host": configuration['connectionBusiness']['dbHost'],
                        "port": configuration['connectionBusiness']['dbPort'],
                        "database": configuration['connectionBusiness']['dbName'],
                        "user": configuration['connectionBusiness']['dbUser'],
                        "password": configuration['connectionBusiness']['dbPassword'],
                    }
                    if logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
                        dbConnectionPrint = dbConnection.copy()
                        dbConnectionPrint.pop("password")
                        logging.debug(json.dumps(dbConnectionPrint))

                logging.debug("IDB query execute: " + json.dumps(queryData['query']))

                if queryData['query'] == 'createAccount':
                    returnValueAccount = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessCreateAccount(
                                                                  queryData), True)
                    if "QueryError" not in returnValueAccount:
                        returnValuePseudonymisation = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessCreateAccountPseudonymisation(
                                                                      queryData), True)
                        returnValueAccount["QueryPseudonymisation"] = returnValuePseudonymisation
                    returnValue = IdbQueryResponse.responseOkDict(returnValueAccount)
                elif queryData['query'] == 'dropCreateAccount':
                    if allowDelete or allowMigration:
                        queryData['executeDbDropSQL'] = True
                        returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessCreateAccount(
                                                                      queryData))
                    else:
                        logging.debug("Disabled for security")
                        returnValue = IdbQueryError.requestUnsupportedService('Disabled for security')
                elif queryData['query'] == 'updateDataTypes':
                    returnValueAccount = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessUpdateDataTypes(
                                                                  queryData), True)
                    if "QueryError" not in returnValueAccount:
                        queryData['dbTableName'] = queryData['dbTableName'] + ".pn"
                        returnValuePseudonymisation = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessUpdateDataTypes(
                                                                      queryData), True)
                        returnValueAccount["QueryPseudonymisation"] = returnValuePseudonymisation
                    returnValue = IdbQueryResponse.responseOkDict(returnValueAccount)
                elif queryData['query'] == 'deleteAccount':
                    returnValueAccount = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteAccount(
                                                                  queryData), True)
                    if "QueryError" not in returnValueAccount:
                        queryData['dbTableName'] = queryData['dbTableName'] + ".pn"
                        returnValuePseudonymisation = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                         IdbSqlQueryBuilder.generateSqlBusinessDeleteAccount(
                                                                             queryData), True)
                        returnValueAccount["QueryPseudonymisation"] = returnValuePseudonymisation
                    returnValue = IdbQueryResponse.responseOkDict(returnValueAccount)
                elif queryData['query'] == 'countAllItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessCountAllItems(
                                                                queryData))
                elif queryData['query'] == 'putItem':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessPutItem(
                                                                queryData), True)
                elif queryData['query'] == 'putItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessPutItems(
                                                                queryData), True)
                elif queryData['query'] == 'getItem':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessGetItem(
                                                                queryData))
                elif queryData['query'] == 'updateItem':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessUpdateItem(
                                                                  queryData))
                elif queryData['query'] == 'deleteItem':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteItem(
                                                                  queryData))
                elif queryData['query'] == 'findItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessFindItems(
                                                                queryData))
                elif queryData['query'] == 'findCountAllItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessCountAllItems(
                                                                dict(queryData)), False, True)
                    if returnValue \
                            and isinstance(returnValue, dict) \
                            and 'Query' in returnValue \
                            and returnValue['Query'] == 1 \
                            and 'QueryData' in returnValue:
                        countAll = returnValue['QueryData']
                        returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                                IdbSqlQueryBuilder.generateSqlBusinessFindItems(
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
                ################################################################################
                # Pseudonymisation                                                             #
                ################################################################################
                elif queryData['query'] == 'recreateAccountPseudonymisation':
                    queryData['executeDbDropSQL'] = True
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessCreateAccountPseudonymisation(
                                                                  queryData))
                elif queryData['query'] == 'putPseudonymisationItem':
                    queryData['dbTableName'] = queryData['dbTableName'] + ".pn"
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessPutItem(
                                                                queryData), True)
                elif queryData['query'] == 'deletePseudonymisationItem':
                    queryData['dbTableName'] = queryData['dbTableName'] + ".pn"
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteItem(
                                                                  queryData))
                elif queryData['query'] == 'findCountAllPseudonymisationItems':
                    queryData['dbTableName'] = queryData['dbTableName'] + ".pn"
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessCountAllItems(
                                                                dict(queryData)), False, True)
                    if returnValue \
                            and isinstance(returnValue, dict) \
                            and 'Query' in returnValue \
                            and returnValue['Query'] == 1 \
                            and 'QueryData' in returnValue:
                        countAll = returnValue['QueryData']
                        returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                                IdbSqlQueryBuilder.generateSqlBusinessFindItems(
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
                ################################################################################
                # Metadata                                                                     #
                ################################################################################
                elif queryData['query'] == 'createAccountMetadata':
                    idb = AwsDynamoDb.idb(configuration['connectionBusiness'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        returnValue = str(idb.createAccountMetadata())
                        if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                            returnValue = IdbQueryResponse.responseCreatedDict({"Metadata Created": 1})
                        elif returnValue and returnValue == 'The conditional request failed':
                            returnValue = IdbQueryError.itemAlreadyExisting()
                        else:
                            returnValue = IdbQueryError.requestInternalServerError()
                elif queryData['query'] == 'setAccountMetadata':
                    idb = AwsDynamoDb.idb(configuration['connectionBusiness'])
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
                elif queryData['query'] == 'getAccountMetadata':
                    idb = AwsDynamoDb.idb(configuration['connectionBusiness'])
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
                elif queryData['query'] == 'deleteAccountMetadata':
                    idb = AwsDynamoDb.idb(configuration['connectionBusiness'])
                    if 'account' in queryData:
                        idb.useAccount(queryData['account'])
                        returnValue = str(idb.deleteAccountMetadata())
                        if returnValue and (returnValue.find("'HTTPStatusCode': 200") > 0):
                            returnValue = IdbQueryResponse.responseCreatedDict({"Metadata Created": 1})
                        elif returnValue and returnValue == 'The conditional request failed':
                            returnValue = IdbQueryError.itemAlreadyExisting()
                        else:
                            returnValue = IdbQueryError.requestInternalServerError()
                ################################################################################
                # ChangeRequest                                                                #
                ################################################################################
                elif queryData['query'] == 'createAccountChangeRequest':
                    queryData['executeDbDropSQL'] = False
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessCreateAccountChangeRequest(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountChangeRequest':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteAccount(
                                                                  queryData))
                elif queryData['query'] == 'dropCreateAccountChangeRequest':
                    if allowDelete or allowMigration:
                        queryData['executeDbDropSQL'] = True
                        returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessCreateAccountChangeRequest(
                                                                      queryData))
                    else:
                        logging.debug("Disabled for security")
                        returnValue = IdbQueryError.requestUnsupportedService('Disabled for security')
                elif queryData['query'] == 'getAllAccountCRs':
                    if 'dbTableLimit' not in queryData:
                        queryData['dbTableLimit'] = 0
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessFindItems(
                                                                queryData))
                elif queryData['query'] == 'getAccountCR':
                    queryData['idbId'] = queryData['id']
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessGetItem(
                                                                queryData))
                elif queryData['query'] == 'getAccountCRbyStatus':
                    if 'dbTableLimit' not in queryData:
                        queryData['dbTableLimit'] = 0
                    queryData["FilterExpression"] = {"o": "=", "l": "#status", "r": ":status"}
                    queryData["ExpressionAttributeNames"] = {"#status": "status"}
                    queryData["ExpressionAttributeValues"] = {":status": queryData['status']}
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessFindItems(
                                                                queryData))
                elif queryData['query'] == 'addAccountCRbyUserId':
                    data = {}
                    if 'userId' in queryData:
                        data["people_id"] = queryData['userId']
                    if 'data' in queryData:
                        data["data"] = queryData['data']
                    if 'status' in queryData:
                        data["status"] = queryData['status']
                    if 'tag' in queryData:
                        data["tag"] = queryData['tag']
                    queryData['data'] = data
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessPutItem(
                                                                queryData), True)
                elif queryData['query'] == 'updateAccountCR':
                    queryData['idbId'] = queryData['id']
                    data = {}
                    # For security reason we do not allow to change stored data as CR is log type for IDBank data
                    # if 'userId' in queryData:
                    #     data["people_id"] = queryData['userId']
                    # if 'data' in queryData:
                    #     data["data"] = queryData['data']
                    if 'status' in queryData:
                        data["status"] = queryData['status']
                    if 'tag' in queryData:
                        data["tag"] = queryData['tag']
                    queryData['data'] = data
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessUpdateItem(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountCR':
                    queryData['idbId'] = queryData['id']
                    # returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                    #                                           IdbSqlQueryBuilder.generateSqlBusinessDeleteItem(
                    #                                               queryData))
                    logging.debug("Disabled for security")
                    returnValue = IdbQueryError.requestUnsupportedService('Disabled for security')
                elif queryData['query'] == 'countAllAccountCRItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessCountAllItems(
                                                                queryData))
                elif queryData['query'] == 'findAccountCRItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessFindItems(
                                                                queryData))
                elif queryData['query'] == 'findCountAllAccountCRItems':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessCountAllItems(
                                                                dict(queryData)), False, True)
                    if returnValue \
                            and isinstance(returnValue, dict) \
                            and 'Query' in returnValue \
                            and returnValue['Query'] == 1 \
                            and 'QueryData' in returnValue:
                        countAll = returnValue['QueryData']
                        returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                                IdbSqlQueryBuilder.generateSqlBusinessFindItems(
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
                ################################################################################
                # Status                                                                       #
                ################################################################################
                elif queryData['query'] == 'createAccountStatus':
                    queryData['executeDbDropSQL'] = False
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessCreateAccountStatus(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountStatus':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteAccount(
                                                                  queryData))
                elif queryData['query'] == 'dropCreateAccountStatus':
                    if allowDelete or allowMigration:
                        queryData['executeDbDropSQL'] = True
                        returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessCreateAccountStatus(
                                                                      queryData))
                    else:
                        logging.debug("Disabled for security")
                        returnValue = IdbQueryError.requestUnsupportedService('Disabled for security')
                elif queryData['query'] == 'getAllAccountSTs':
                    if 'dbTableLimit' not in queryData:
                        queryData['dbTableLimit'] = 0
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessFindItems(
                                                                queryData))
                elif queryData['query'] == 'getAccountSTbyUserId':
                    queryData['dbTableColumnPk'] = "people_id"
                    queryData['idbId'] = queryData['userId']
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessGetItem(
                                                                queryData))
                elif queryData['query'] == 'getAccountSTbyStatus':
                    if 'dbTableLimit' not in queryData:
                        queryData['dbTableLimit'] = 0
                    queryData["FilterExpression"] = {"o": "=", "l": "#status", "r": ":status"}
                    queryData["ExpressionAttributeNames"] = {"#status": "status"}
                    queryData["ExpressionAttributeValues"] = {":status": queryData['status']}
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessFindItems(
                                                                queryData))
                elif queryData['query'] == 'addAccountSTbyUserId':
                    data = {}
                    if 'userId' in queryData:
                        data["people_id"] = queryData['userId']
                    if 'data' in queryData:
                        data["data"] = queryData['data']
                    if 'status' in queryData:
                        data["status"] = queryData['status']
                    if 'tag' in queryData:
                        data["tag"] = queryData['tag']
                    data["updated_at"] = 'now()'
                    queryData['data'] = data
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessPutItem(
                                                                queryData), True)
                elif queryData['query'] == 'updateAccountSTbyUserId':
                    queryData['dbTableColumnPk'] = "people_id"
                    queryData['idbId'] = queryData['userId']
                    data = {}
                    if 'userId' in queryData:
                        data["people_id"] = queryData['userId']
                    if 'data' in queryData:
                        data["data"] = queryData['data']
                    if 'status' in queryData:
                        data["status"] = queryData['status']
                    if 'tag' in queryData:
                        data["tag"] = queryData['tag']
                    data["updated_at"] = 'now()'
                    queryData['data'] = data
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessUpdateItem(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountSTbyUserId':
                    queryData['dbTableColumnPk'] = "people_id"
                    queryData['idbId'] = queryData['userId']
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteItem(
                                                                  queryData))
                ################################################################################
                # Event                                                                        #
                ################################################################################
                elif queryData['query'] == 'createAccountEvents':
                    queryData['executeDbDropSQL'] = False
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessCreateAccountEvent(
                                                                  queryData))

                elif queryData['query'] == 'deleteAccountEvents':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteAccount(
                                                                  queryData))
                elif queryData['query'] == 'dropCreateAccountEvents':
                    if allowDelete or allowMigration:
                        queryData['executeDbDropSQL'] = True
                        returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessCreateAccountEvent(
                                                                      queryData))
                    else:
                        logging.debug("Disabled for security")
                        returnValue = IdbQueryError.requestUnsupportedService('Disabled for security')
                elif queryData['query'] == 'addAccountEvent':
                    data = {}
                    if 'oid' in queryData:
                        data["oid"] = queryData['oid']
                    if 'type' in queryData:
                        data["type"] = queryData['type']
                    if 'event_time' in queryData:
                        data["event_time"] = queryData['event_time']
                    if 'event_action' in queryData:
                        data["event_action"] = queryData['event_action']
                    if 'tag' in queryData:
                        data["tag"] = queryData['tag']
                    if 'metadata' in queryData:
                        data["metadata"] = queryData['metadata']
                    if 'attributes' in queryData:
                        data["attributes"] = queryData['attributes']
                    if 'security' in queryData:
                        data["security"] = queryData['security']
                    queryData['data'] = data
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessPutItem(
                                                                queryData), True)
                elif queryData['query'] == 'updateAccountEvent':
                    queryData['idbId'] = queryData['id']
                    data = {}
                    if 'event_time' in queryData:
                        data["event_time"] = queryData['event_time']
                    if 'event_action' in queryData:
                        data["event_action"] = queryData['event_action']
                    if 'tag' in queryData:
                        data["tag"] = queryData['tag']
                    if 'attributes' in queryData:
                        data["attributes"] = queryData['attributes']
                    if 'security' in queryData:
                        data["security"] = queryData['security']
                    data["updated_at"] = 'now()'
                    queryData['data'] = data
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessUpdateItem(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountEventsCondition':
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlGenericDeleteItems(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountEvent':
                    queryData['idbId'] = queryData['id']
                    returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                              IdbSqlQueryBuilder.generateSqlBusinessDeleteItem(
                                                                  queryData))
                elif queryData['query'] == 'deleteAccountEventsByObjectId':
                    queryData['dbTableColumnPk'] = "oid"
                    if 'oid' in queryData:
                        queryData['dbTablePk'] = sql.Literal(str(queryData['oid']))
                        returnValue = IdbQuerySql.executeSqlQuery(dbConnection,
                                                                  IdbSqlQueryBuilder.generateSqlBusinessDeleteItem(
                                                                      queryData))
                elif queryData['query'] == 'findCountAllAccountEvents':
                    returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                            IdbSqlQueryBuilder.generateSqlBusinessCountAllItems(
                                                                dict(queryData)), False, True)
                    if returnValue \
                            and isinstance(returnValue, dict) \
                            and 'Query' in returnValue \
                            and returnValue['Query'] == 1 \
                            and 'QueryData' in returnValue:
                        countAll = returnValue['QueryData']
                        returnValue = IdbQuerySql.fetchSqlQuery(dbConnection,
                                                                IdbSqlQueryBuilder.generateSqlBusinessFindItems(
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
