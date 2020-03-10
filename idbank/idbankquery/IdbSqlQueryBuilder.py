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

import re
import logging
import collections
from psycopg2 import sql


################################################################################
# Module                                                                       #
################################################################################

class IdbSqlQueryBuilder:

    @staticmethod
    def generateSqlSetRelationBusiness2People(queryData: dict) -> str:
        querySql = sql.SQL(
            """INSERT
            INTO {dbTableSchemaIdentifier}.{dbTableNameIdentifier}
            ({bidIdentifier}, {pidIdentifier})
            VALUES
            ({businessIdPlaceholder}, {peopleIdPlaceholder});""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlDeleteRelationBusiness2People(queryData: dict) -> str:
        querySql = sql.SQL(
            """DELETE FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier} {dbTableCondition};""").format(**queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlCheckRelationBusiness2People(queryData: dict) -> str:
        querySql = sql.SQL(
            """SELECT EXISTS
            (SELECT 1 FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier}
            {dbTableCondition});""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlCheckRelationsBusiness2People(queryData: dict) -> str:
        if "relations" in queryData and isinstance(queryData["relations"], list):
            sqlQueryList = []
            dbTableValues = {}
            relationIndex = 0
            for relation in queryData["relations"]:
                relationIndex += 1
                bidColumnPlaceholder = 'col_bid_{}'.format(relationIndex)
                pidColumnPlaceholder = 'col_pid_{}'.format(relationIndex)
                dbTableValues[bidColumnPlaceholder] = relation['businessId']
                dbTableValues[pidColumnPlaceholder] = relation['peopleId']
                queryData['businessIdPlaceholder'] = sql.Placeholder(bidColumnPlaceholder)
                queryData['peopleIdPlaceholder'] = sql.Placeholder(pidColumnPlaceholder)
                queryData['dbTableCondition'] = sql.SQL("""
                WHERE
                {bidIdentifier} LIKE {businessIdPlaceholder}
                AND
                {pidIdentifier} LIKE {peopleIdPlaceholder}""").format(**queryData)
                sqlPrefix = ' UNION ALL '
                if relationIndex == 1:
                    sqlPrefix = ''
                querySql = sql.SQL(
                    sqlPrefix + """SELECT EXISTS
                    (SELECT 1
                    FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier}
                    {dbTableCondition})""").format(
                    **queryData)
                sqlQueryList.append(querySql)
            return {'sql': sqlQueryList, 'data': dbTableValues}
        else:
            raise Exception('Bad query.')

    @staticmethod
    def generateSqlGetRelatedPeoples(queryData: dict) -> str:
        queryData = IdbSqlQueryBuilder.generateSqlPagination(queryData)
        if 'selectAll' in queryData and queryData['selectAll']:
            queryData['selectIdentifier'] = sql.SQL('*')
        else:
            queryData['selectIdentifier'] = queryData['pidIdentifier']
        querySql = sql.SQL(
            """SELECT
            {selectIdentifier}
            FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier}
            {dbTableCondition}
            ORDER BY {pidIdentifier} ASC
            {dbTableLimit} {dbTableOffset};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlGetRelatedBusinesses(queryData: dict) -> str:
        queryData = IdbSqlQueryBuilder.generateSqlPagination(queryData)
        if 'selectAll' in queryData and queryData['selectAll']:
            queryData['selectIdentifier'] = sql.SQL('*')
        else:
            queryData['selectIdentifier'] = queryData['bidIdentifier']
        querySql = sql.SQL(
            """SELECT
            {selectIdentifier}
            FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier}
            {dbTableCondition}
            ORDER BY {bidIdentifier} ASC
            {dbTableLimit} {dbTableOffset};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def convertBusinessAccountTypeToSql(type: str) -> str:
        if type == 'string':
            return 'text'
        elif type == 'integer':
            return 'integer'
        elif type == 'boolean':
            return 'smallint'
        elif type == 'numeric':
            return 'numeric'
        elif type == 'float':
            return 'double'
        elif type == 'time':
            return 'time'
        elif type == 'timetz':
            return 'timetz'
        elif type == 'timestamp':
            return 'timestamp'
        elif type == 'timestamptz':
            return 'timestamptz'
        elif type == 'timedelta':
            return 'interval'
        else:
            return 'text'

    @staticmethod
    def generateSqlBusinessCreateAccount(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])

        queryData['dbTableColumns'] = []
        queryData['dbTableColumns'].append(sql.SQL('"id" serial PRIMARY KEY'))

        if 'DataTypes' in queryData and 'database' in queryData['DataTypes']:
            queryData['DataTypes']['dbTableColumns'] = []
            queryData['DataTypes']['dbTableColumnsType'] = {}
            for column in queryData['DataTypes']['database']:
                queryData['DataTypes']['dbTableColumns'].append(column['uuid'])
                queryData['DataTypes']['dbTableColumnsType'][
                    column['uuid']] = IdbSqlQueryBuilder.convertBusinessAccountTypeToSql(column['type'])

            queryData['dbTableColumns'] += (
                sql.SQL('{} ' + queryData['DataTypes']['dbTableColumnsType'][column]).format(sql.Identifier(column)) for
                column in queryData['DataTypes']['dbTableColumns'])

        queryData['dbTableColumns'] = sql.SQL(', ').join(queryData['dbTableColumns'])

        return IdbSqlQueryBuilder.generateSqlBusinessDropCreateIdbank(queryData)

    @staticmethod
    def generateSqlBusinessUpdateDataTypes(queryData: dict) -> str:
        returnSql = None
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        if 'DataTypes' in queryData and 'database' in queryData['DataTypes']:
            queryData['DataTypes']['actionsSql'] = list()
            for actions in queryData['DataTypes']['database']:
                if actions == 'add':
                    queryData['DataTypes']['actionsSql'] += IdbSqlQueryBuilder.generateSqlBusinessUpdateDataTypesAdd(
                        queryData['DataTypes']['database'][actions], queryData)
                elif actions == 'drop':
                    queryData['DataTypes']['actionsSql'] += IdbSqlQueryBuilder.generateSqlBusinessUpdateDataTypesDrop(
                        queryData['DataTypes']['database'][actions], queryData)
                elif actions == 'rename':
                    queryData['DataTypes']['actionsSql'] += IdbSqlQueryBuilder.generateSqlBusinessUpdateDataTypesRename(
                        queryData['DataTypes']['database'][actions], queryData)
                elif actions == 'update':
                    queryData['DataTypes']['actionsSql'] += IdbSqlQueryBuilder.generateSqlBusinessUpdateDataTypesUpdate(
                        queryData['DataTypes']['database'][actions], queryData)

            if 0 < len(queryData['DataTypes']['actionsSql']):
                returnSql = queryData['DataTypes']['actionsSql']
        if not returnSql:
            raise Exception('Empty update query.')
        return {'sql': returnSql, 'data': queryData}

    @staticmethod
    def generateSqlBusinessUpdateDataTypesAdd(actions: collections.Iterable, queryData: dict):
        returnSqlArray = list()
        for action in actions:
            queryData["columnName"] = sql.Identifier(action['uuid'])
            querySql = sql.SQL(
                """ALTER TABLE {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} ADD COLUMN IF NOT EXISTS {{columnName}} {columnType};""".format_map(
                    {"columnType": IdbSqlQueryBuilder.convertBusinessAccountTypeToSql(action['type'])})
            ).format(
                **queryData)
            returnSqlArray.append(querySql)
        return returnSqlArray

    @staticmethod
    def generateSqlBusinessUpdateDataTypesDrop(actions: collections.Iterable, queryData: dict):
        returnSqlArray = list()
        for action in actions:
            queryData["columnName"] = sql.Identifier(action['uuid'])
            querySql = sql.SQL(
                """ALTER TABLE {dbTableSchemaIdentifier}.{dbTableNameIdentifier} DROP COLUMN IF EXISTS {columnName};""").format(
                **queryData)
            returnSqlArray.append(querySql)
        return returnSqlArray

    @staticmethod
    def generateSqlBusinessUpdateDataTypesRename(actions: collections.Iterable, queryData: dict):
        returnSqlArray = list()
        for action in actions:
            queryData["fromColumnName"] = sql.Identifier(action['from'])
            queryData["toColumnName"] = sql.Identifier(action['to'])
            querySql = sql.SQL(
                """ALTER TABLE {dbTableSchemaIdentifier}.{dbTableNameIdentifier} RENAME COLUMN {fromColumnName} TO {toColumnName};""").format(
                **queryData)
            returnSqlArray.append(querySql)
        return returnSqlArray

    @staticmethod
    def convertBusinessAccountTypeToSqlCast(type: str) -> str:
        type = IdbSqlQueryBuilder.convertBusinessAccountTypeToSql(type)
        action = {}
        if type == 'integer':
            action['columnType'] = 'integer'
            action['columnCast'] = 'USING (trim({columnName})::integer)'
        elif type == 'smallint':
            action['columnType'] = 'smallint'
            action['columnCast'] = 'USING (trim({columnName})::smallint)'
        else:
            action['columnType'] = 'text'
            action['columnCast'] = ''
        return action

    @staticmethod
    def generateSqlBusinessUpdateDataTypesUpdate(actions: collections.Iterable, queryData: dict):
        returnSqlArray = list()
        for action in actions:
            cast = IdbSqlQueryBuilder.convertBusinessAccountTypeToSqlCast(action['type'])
            queryData["columnName"] = sql.Identifier(action['uuid'])
            querySql = sql.SQL(
                """ALTER TABLE {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} ALTER COLUMN {{columnName}} TYPE {columnType} {columnCast};""".format(
                    **cast)).format(
                **queryData)
            returnSqlArray.append(querySql)
        return returnSqlArray

    @staticmethod
    def generateSqlBusinessDeleteAccount(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        querySql = sql.SQL(
            """DROP TABLE {dbTableSchemaIdentifier}.{dbTableNameIdentifier};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlCountAllItems(queryData: dict) -> str:
        if 'dbTableCondition' not in queryData:
            queryData['dbTableCondition'] = sql.SQL('')
        querySql = sql.SQL(
            """SELECT COUNT(*) FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier} {dbTableCondition};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlBusinessCountAllItems(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        queryData = IdbSqlQueryBuilder.generateSqlBusinessTableCondition(queryData)
        return IdbSqlQueryBuilder.generateSqlCountAllItems(queryData)

    @staticmethod
    def generateSqlBusinessPutItem(queryData: dict) -> str:
        return IdbSqlQueryBuilder.generateSqlGenericPutItem(queryData)

    @staticmethod
    def generateSqlGenericPutItem(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        columnIndex = 0
        if 'data' in queryData and isinstance(queryData['data'], dict):
            if 'dbTableColumnPk' not in queryData:
                queryData['dbTableColumnPk'] = 'id'
            queryData['dbTableColumns'] = []
            queryData['dbTableValuesPlaceholders'] = []
            dbTableValues = {}

            if 'idbId' in queryData and isinstance(queryData['idbId'], int):
                queryData['dbTableColumns'].append(queryData['dbTableColumnPk'])
                columnPlaceholder = 'col_id'
                queryData['dbTableValuesPlaceholders'].append(columnPlaceholder)
                dbTableValues[columnPlaceholder] = queryData['idbId']

            for columnName, columnValue in queryData['data'].items():
                columnIndex += 1
                queryData['dbTableColumns'].append(columnName)
                columnPlaceholder = 'col_{}'.format(columnIndex)
                queryData['dbTableValuesPlaceholders'].append(columnPlaceholder)
                dbTableValues[columnPlaceholder] = columnValue

            queryData['dbTableColumns'] = sql.SQL(', ').join(map(sql.Identifier, queryData['dbTableColumns']))
            queryData['dbTableValuesPlaceholders'] = sql.SQL(', ').join(
                map(sql.Placeholder, queryData['dbTableValuesPlaceholders']))
            queryData['dbTableColumnPk'] = sql.Identifier(queryData['dbTableColumnPk'])
            querySql = sql.SQL(
                """INSERT INTO {dbTableSchemaIdentifier}.{dbTableNameIdentifier} ({dbTableColumns}) VALUES ({dbTableValuesPlaceholders}) RETURNING {dbTableColumnPk};""").format(
                **queryData)
            return {'sql': querySql, 'data': dbTableValues}
        else:
            raise Exception('Bad query.')

    @staticmethod
    def generateSqlBusinessPutItems(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        if 'dbTableColumnPk' not in queryData:
            queryData['dbTableColumnPk'] = 'id'
        queryData['dbTableColumnPk'] = sql.Identifier(queryData['dbTableColumnPk'])
        if 'data' in queryData and isinstance(queryData['data'], list):
            sqlQueryList = []
            dbTableValues = {}
            itemIndex = 0
            for item in queryData['data']:
                itemIndex += 1
                queryData['dbTableColumns'] = []
                queryData['dbTableValuesPlaceholders'] = []
                columnIndex = 0
                for columnName, columnValue in item.items():
                    columnIndex += 1
                    queryData['dbTableColumns'].append(columnName)
                    columnPlaceholder = 'col_{}_{}'.format(itemIndex, columnIndex)
                    queryData['dbTableValuesPlaceholders'].append(columnPlaceholder)
                    dbTableValues[columnPlaceholder] = columnValue

                queryData['dbTableColumns'] = sql.SQL(', ').join(map(sql.Identifier, queryData['dbTableColumns']))
                queryData['dbTableValuesPlaceholders'] = sql.SQL(', ').join(
                    map(sql.Placeholder, queryData['dbTableValuesPlaceholders']))
                querySql = sql.SQL(
                    """INSERT INTO {dbTableSchemaIdentifier}.{dbTableNameIdentifier} ({dbTableColumns}) VALUES ({dbTableValuesPlaceholders}) RETURNING {dbTableColumnPk};""").format(
                    **queryData)
                sqlQueryList.append(querySql)
            return {'sql': sqlQueryList, 'data': dbTableValues}
        else:
            raise Exception('Bad query.')

    @staticmethod
    def generateSqlBusinessGetItem(queryData: dict) -> str:
        return IdbSqlQueryBuilder.generateSqlGenericGetItem(queryData)

    @staticmethod
    def generateSqlGenericGetItem(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        if 'dbTableColumnPk' not in queryData:
            queryData['dbTableColumnPk'] = 'id'
        if 'idbId' in queryData and isinstance(queryData['idbId'], int):
            queryData['dbTablePk'] = sql.SQL(str(queryData['idbId']))
        if 'dbTableColumns' not in queryData:
            queryData['dbTableColumns'] = sql.SQL('*')
        if 'DataTypes' in queryData and queryData['DataTypes'] and \
                'database' in queryData['DataTypes'] and \
                isinstance(queryData['DataTypes']['database'], list) and \
                0 < len(queryData['DataTypes']['database']):
            queryData['dbTableColumns'] = sql.SQL(', ').join(
                sql.Identifier(column) for column in queryData['DataTypes']['database'])
        queryData['dbTableColumnPk'] = sql.Identifier(queryData['dbTableColumnPk'])
        querySql = sql.SQL(
            """SELECT {dbTableColumns} FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier} WHERE {dbTableColumnPk} = {dbTablePk};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlBusinessUpdateItem(queryData: dict) -> str:
        return IdbSqlQueryBuilder.generateSqlGenericUpdateItem(queryData)

    @staticmethod
    def generateSqlGenericUpdateItem(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        if 'dbTableColumnPk' not in queryData:
            queryData['dbTableColumnPk'] = 'id'
        if 'idbId' in queryData and isinstance(queryData['idbId'], int):
            queryData['dbTablePk'] = sql.SQL(str(queryData['idbId']))

        columnIndex = 0
        if 'data' in queryData and isinstance(queryData['data'], dict):
            dbTableValues = {}
            queryData['dbTableColumnsValues'] = []

            for columnName, columnValue in queryData['data'].items():
                columnIndex += 1
                columnPlaceholder = 'col_{}'.format(columnIndex)
                queryData['dbTableColumnsValues'].append(
                    sql.SQL("{} = {}").format(sql.Identifier(columnName), sql.Placeholder(columnPlaceholder)))
                dbTableValues[columnPlaceholder] = columnValue

            queryData['dbTableColumnsValues'] = sql.SQL(', ').join(queryData['dbTableColumnsValues'])
            queryData['dbTableColumnPk'] = sql.Identifier(queryData['dbTableColumnPk'])

        querySql = sql.SQL(
            """UPDATE {dbTableSchemaIdentifier}.{dbTableNameIdentifier} SET {dbTableColumnsValues} WHERE {dbTableColumnPk} = {dbTablePk};""").format(
            **queryData)
        return {'sql': querySql, 'data': dbTableValues}

    @staticmethod
    def generateSqlBusinessDeleteItem(queryData: dict) -> str:
        return IdbSqlQueryBuilder.generateSqlGenericDeleteItem(queryData)

    @staticmethod
    def generateSqlGenericDeleteItem(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        if 'dbTableColumnPk' not in queryData:
            queryData['dbTableColumnPk'] = 'id'
        queryData['dbTableColumnPk'] = sql.Identifier(queryData['dbTableColumnPk'])
        if 'idbId' in queryData and isinstance(queryData['idbId'], int):
            queryData['dbTablePk'] = sql.SQL(str(queryData['idbId']))

        querySql = sql.SQL(
            """DELETE FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier} WHERE {dbTableColumnPk} = {dbTablePk};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def orderString(type: str) -> str:
        if type.upper() == 'DESC':
            return 'DESC'
        else:
            return 'ASC'

    @staticmethod
    def generateSqlFilterExpressionString(filterExpression: dict) -> str:
        filterExpressionStringOperator = None
        filterExpressionStringLeft = None
        filterExpressionStringRight = None
        if 'o' in filterExpression:
            filterExpressionStringOperator = re.sub(r"\s+", "", filterExpression['o'], flags=re.UNICODE)
        if 'l' in filterExpression:
            filterExpressionStringLeft = filterExpression['l']
            if isinstance(filterExpressionStringLeft, dict):
                filterExpressionStringLeft = IdbSqlQueryBuilder.generateSqlFilterExpressionString(
                    filterExpressionStringLeft)
            else:
                filterExpressionStringLeft = "{" + filterExpressionStringLeft + '}'
            filterExpressionStringLeft = filterExpressionStringLeft.strip()
        if 'r' in filterExpression:
            filterExpressionStringRight = filterExpression['r']
            if isinstance(filterExpressionStringRight, dict):
                filterExpressionStringRight = IdbSqlQueryBuilder.generateSqlFilterExpressionString(
                    filterExpressionStringRight)
            else:
                filterExpressionStringRight = "{" + filterExpressionStringRight + '}'
            filterExpressionStringRight = filterExpressionStringRight.strip()

        if filterExpressionStringLeft is None or \
                filterExpressionStringOperator is None or \
                filterExpressionStringRight is None:
            return None

        filterExpressionString = '{l} {o} {r}'.format(**{'l': filterExpressionStringLeft,
                                                         'o': filterExpressionStringOperator,
                                                         'r': filterExpressionStringRight})

        if 'b' in filterExpression and filterExpression['b']:
            filterExpressionString = "(" + filterExpressionString + ')'

        return filterExpressionString

    @staticmethod
    def generateSqlBusinessTableCondition(queryData: dict) -> str:
        return IdbSqlQueryBuilder.generateSqlGenericTableCondition(queryData)

    @staticmethod
    def generateSqlGenericTableCondition(queryData: dict) -> str:
        expressionNames = {}
        expressionValue = {}
        if 'dbTableConditionString' in queryData and queryData['dbTableConditionString']:
            logging.debug("Allow find condition as string!")
        elif 'FilterExpression' in queryData and queryData['FilterExpression']:
            queryData['FilterExpression'] = IdbSqlQueryBuilder.generateSqlFilterExpressionString(
                queryData['FilterExpression'])

        if ('dbTableCondition' not in queryData or \
            queryData['dbTableCondition'] == None or \
            queryData['dbTableCondition'] == '' or \
            queryData['dbTableCondition'] == sql.SQL('')) and \
                'FilterExpression' in queryData and queryData[
            'FilterExpression']:
            queryData['dbTableCondition'] = queryData['FilterExpression']
            queryData['dbTableCondition'] = queryData['dbTableCondition'].replace('{#', '{sqlIdentifierIdb_')
            queryData['dbTableCondition'] = queryData['dbTableCondition'].replace('{:', '{sqlValueIdb_')
            queryData['dbTableCondition'] = sql.SQL(queryData['dbTableCondition'])
            if 'ExpressionAttributeNames' in queryData and queryData['ExpressionAttributeNames']:
                for key in queryData['ExpressionAttributeNames'].keys():
                    expressionNames[('{' + key).replace('{#', 'sqlIdentifierIdb_')] = sql.Identifier(
                        queryData['ExpressionAttributeNames'][key])
                for key in queryData['ExpressionAttributeValues'].keys():
                    keyItem = ('{' + key).replace('{:', 'sqlValueIdb_')
                    expressionNames[keyItem] = sql.Placeholder(keyItem)
                    expressionValue[keyItem] = queryData['ExpressionAttributeValues'][key]

            queryData['dbTableCondition'] = (queryData['dbTableCondition']).format(**expressionNames)

        if 'dbTableCondition' not in queryData or \
                queryData['dbTableCondition'] is None or \
                queryData['dbTableCondition'] == '' or \
                queryData['dbTableCondition'] == sql.SQL(''):
            queryData['dbTableCondition'] = sql.SQL('')
        else:
            queryData['dbTableCondition'] = sql.SQL('WHERE ({})').format(queryData['dbTableCondition'])
        queryData.update(expressionValue)
        return queryData

    @staticmethod
    def generateSqlColumnOrder(queryData: dict) -> str:
        if 'dbTableOrder' not in queryData or queryData['dbTableOrder'] is None:
            queryData['dbTableOrder'] = sql.Identifier('id')
        if 'OrderByDataTypes' in queryData and queryData['OrderByDataTypes']:
            queryData['dbTableOrder'] = sql.SQL(', ').join(
                sql.SQL("{0} {1}").format(sql.Identifier(column), sql.SQL(
                    IdbSqlQueryBuilder.orderString(queryData['OrderByDataTypes'][column]))) for
                column in
                queryData['OrderByDataTypes'].keys())
        queryData['dbTableOrder'] = sql.SQL('ORDER BY {dbTableOrder}').format(**queryData)
        return queryData

    @staticmethod
    def generateSqlPagination(queryData: dict) -> str:
        if 'PaginationConfig' in queryData and queryData['PaginationConfig']:
            if 'PageSize' in queryData['PaginationConfig']:
                if queryData['PaginationConfig']['PageSize']:
                    queryData['dbTableLimit'] = int(queryData['PaginationConfig']['PageSize'])
                else:
                    queryData['dbTableLimit'] = sql.SQL('')
                    queryData['dbTableOffset'] = sql.SQL('')
                    return queryData
            if 'Page' in queryData['PaginationConfig'] and queryData['PaginationConfig']['Page']:
                queryData['dbTableOffset'] = int(queryData['PaginationConfig']['Page']) * queryData['dbTableLimit']
        if 'dbTableLimit' not in queryData:
            queryData['dbTableLimit'] = 2
        if queryData['dbTableLimit'] == 0:
            queryData['dbTableLimit'] = sql.SQL('')
        else:
            queryData['dbTableLimit'] = sql.SQL(str(queryData['dbTableLimit']))
            queryData['dbTableLimit'] = sql.SQL('LIMIT {dbTableLimit}').format(**queryData)
        if 'dbTableOffset' not in queryData:
            queryData['dbTableOffset'] = sql.SQL('')
        else:
            queryData['dbTableOffset'] = sql.SQL(str(queryData['dbTableOffset']))
            queryData['dbTableOffset'] = sql.SQL('OFFSET {dbTableOffset}').format(**queryData)
        return queryData

    @staticmethod
    def generateSqlBusinessFindItems(queryData: dict) -> str:
        return IdbSqlQueryBuilder.generateSqlGenericFindItems(queryData)

    @staticmethod
    def generateSqlGenericFindItems(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])

        if 'dbTableColumns' not in queryData:
            queryData['dbTableColumns'] = sql.SQL('*')
        if 'DataTypes' in queryData and queryData['DataTypes'] and \
                'database' in queryData['DataTypes'] and \
                isinstance(queryData['DataTypes']['database'], list) and \
                0 < len(queryData['DataTypes']['database']):
            queryData['dbTableColumns'] = sql.SQL(', ').join(
                sql.Identifier(column) for column in queryData['DataTypes']['database'])

        queryData = IdbSqlQueryBuilder.generateSqlColumnOrder(queryData)
        queryData = IdbSqlQueryBuilder.generateSqlPagination(queryData)
        queryData = IdbSqlQueryBuilder.generateSqlBusinessTableCondition(queryData)

        querySql = sql.SQL(
            """SELECT {dbTableColumns} FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier} {dbTableCondition} {dbTableOrder} {dbTableLimit} {dbTableOffset};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlGenericDeleteItems(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])
        queryData = IdbSqlQueryBuilder.generateSqlBusinessTableCondition(queryData)

        querySql = sql.SQL(
            """DELETE FROM {dbTableSchemaIdentifier}.{dbTableNameIdentifier} {dbTableCondition};""").format(
            **queryData)
        return {'sql': querySql, 'data': queryData}

    @staticmethod
    def generateSqlBusinessCreateAccountChangeRequest(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])

        queryData['dbTableColumns'] = []
        queryData['dbTableColumns'].append(sql.SQL('"id" serial PRIMARY KEY'))
        queryData['dbTableColumns'].append(sql.SQL('"idb_version" int NOT NULL DEFAULT 1'))
        queryData['dbTableColumns'].append(sql.SQL('"people_id" int NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"data" text NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"created_at" timestamp without time zone DEFAULT now()'))
        queryData['dbTableColumns'].append(sql.SQL('"tag" varchar(255)'))
        queryData['dbTableColumns'].append(sql.SQL('"status" varchar(255)'))
        queryData['dbTableColumns'] = sql.SQL(', ').join(queryData['dbTableColumns'])

        queryData['postSQL'] = sql.SQL(("""CREATE INDEX "{dbTableName}_idx_people_id" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (people_id);
                                           CREATE INDEX "{dbTableName}_idx_created_at" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (created_at);
                                           CREATE INDEX "{dbTableName}_idx_tag" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (tag);
                                           CREATE INDEX "{dbTableName}_idx_status" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (status);"""
                                       ).format(**queryData)).format(**queryData)

        return IdbSqlQueryBuilder.generateSqlBusinessDropCreateIdbank(queryData)

    @staticmethod
    def generateSqlBusinessCreateAccountStatus(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])

        queryData['dbTableColumns'] = []
        queryData['dbTableColumns'].append(sql.SQL('"id" serial PRIMARY KEY'))
        queryData['dbTableColumns'].append(sql.SQL('"idb_version" int NOT NULL DEFAULT 1'))
        queryData['dbTableColumns'].append(sql.SQL('"people_id" int NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"data" text NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"created_at" timestamp without time zone DEFAULT now()'))
        queryData['dbTableColumns'].append(sql.SQL('"updated_at" timestamp without time zone DEFAULT now()'))
        queryData['dbTableColumns'].append(sql.SQL('"tag" varchar(255)'))
        queryData['dbTableColumns'].append(sql.SQL('"status" varchar(255)'))
        queryData['dbTableColumns'] = sql.SQL(', ').join(queryData['dbTableColumns'])

        queryData['postSQL'] = sql.SQL(("""CREATE UNIQUE INDEX "{dbTableName}_idx_people_id" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (people_id);
                                           CREATE INDEX "{dbTableName}_idx_created_at" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (created_at);
                                           CREATE INDEX "{dbTableName}_idx_updated_at" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (updated_at);
                                           CREATE INDEX "{dbTableName}_idx_tag" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (tag);
                                           CREATE INDEX "{dbTableName}_idx_status" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (status);"""
                                       ).format(**queryData)).format(**queryData)

        return IdbSqlQueryBuilder.generateSqlBusinessDropCreateIdbank(queryData)

    @staticmethod
    def generateSqlBusinessCreateAccountEvent(queryData: dict) -> str:
        queryData['businessDbId'] = queryData['account']
        queryData['dbTableName'] = queryData['dbTableName'].format(**queryData)
        queryData['dbTableNameIdentifier'] = sql.Identifier(queryData['dbTableName'])

        queryData['dbTableColumns'] = []
        queryData['dbTableColumns'].append(sql.SQL('"id" serial PRIMARY KEY'))
        queryData['dbTableColumns'].append(sql.SQL('"idb_version" int NOT NULL DEFAULT 1'))
        queryData['dbTableColumns'].append(sql.SQL('"oid" varchar(1024) NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"type" varchar(255) NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"created_at" timestamp with time zone DEFAULT now()'))
        queryData['dbTableColumns'].append(sql.SQL('"updated_at" timestamp with time zone DEFAULT now()'))
        queryData['dbTableColumns'].append(sql.SQL('"event_time" timestamp with time zone'))
        queryData['dbTableColumns'].append(sql.SQL('"event_action" text NOT NULL'))
        queryData['dbTableColumns'].append(sql.SQL('"tag" varchar(255)'))
        queryData['dbTableColumns'].append(sql.SQL('"metadata" text'))
        queryData['dbTableColumns'].append(sql.SQL('"attributes" text'))
        queryData['dbTableColumns'].append(sql.SQL('"security" text'))
        queryData['dbTableColumns'] = sql.SQL(', ').join(queryData['dbTableColumns'])

        queryData['postSQL'] = sql.SQL(("""CREATE INDEX "{dbTableName}_idx_oid" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (oid);
                                           CREATE INDEX "{dbTableName}_idx_type" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (type);
                                           CREATE INDEX "{dbTableName}_idx_created_at" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (created_at);
                                           CREATE INDEX "{dbTableName}_idx_updated_at" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (updated_at);
                                           CREATE INDEX "{dbTableName}_idx_tag" ON {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}} (tag);"""
                                       ).format(**queryData)).format(**queryData)

        return IdbSqlQueryBuilder.generateSqlBusinessDropCreateIdbank(queryData)

    @staticmethod
    def generateSqlBusinessDropCreateIdbank(queryData: dict) -> str:

        if 'executeDbDropSQL' in queryData and queryData['executeDbDropSQL']:
            queryData['preSQL'] = sql.SQL(("""DROP TABLE IF EXISTS {{dbTableSchemaIdentifier}}.{{dbTableNameIdentifier}};"""
                                              ).format(**queryData)).format(**queryData)

        return IdbSqlQueryBuilder.generateSqlGenericCreateSql(queryData)

    @staticmethod
    def generateSqlGenericCreateSql(queryData: dict) -> str:

        if not ('preSQL' in queryData and queryData['preSQL']):
            queryData['preSQL'] = sql.SQL("")
        if not ('postSQL' in queryData and queryData['postSQL']):
            queryData['postSQL'] = sql.SQL("")

        querySql = sql.SQL(
            """{preSQL}
               CREATE TABLE {dbTableSchemaIdentifier}.{dbTableNameIdentifier} ({dbTableColumns});
               ALTER TABLE {dbTableSchemaIdentifier}.{dbTableNameIdentifier} OWNER TO idb_data;
               {postSQL}
               """).format(**queryData)

        return {'sql': querySql, 'data': queryData}

################################################################################
#                                End of file                                   #
################################################################################
