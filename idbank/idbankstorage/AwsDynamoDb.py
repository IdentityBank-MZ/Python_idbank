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
import decimal
import logging
import boto3
from botocore.exceptions import ClientError

from idbank import IdbCommon, IdbConfig
from .IdbankStorageBase import IdbankStorageBase
from .IdbankStorageFormat import IdbankStorageFormat, IdbankStorageType, IdbankStorageInfo, IdbankStorageTags
from .IdbankStorageEngine import IdbankStorageEngine


################################################################################
# Module                                                                       #
################################################################################

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class AwsDynamoDb(IdbankStorageBase):
    region_name = None
    aws_access_key_id = None
    aws_secret_access_key = None
    dynamodbClient = None
    dynamodbResource = None

    dbKeyNames = {
        'account': 'idbaccount',
        'id': 'idbid',
        'public': 'idbpublic',
        'protected': 'idbprotected',
        'private': 'idbprivate'
    }

    attributesKeyNames = {
        'metadata': 'idbmetadata',
        'delete': 'idbdelete',
        'item': 'idbitem',
        'events': 'idbevents',
        'assets': 'idbitem',
    }

    def __init__(self, configuration):
        IdbankStorageBase.__init__(self, configuration)
        try:
            self.region_name = self.configuration['region_name']
            self.aws_access_key_id = self.configuration['aws_access_key_id']
            self.aws_secret_access_key = self.configuration['aws_secret_access_key']
        except:
            errorMessage = 'There is not all the required data to connect to the AWS Dynamo DB.'
            logging.error(errorMessage)
            raise ValueError(errorMessage)

    def initClient(self):
        if self.dynamodbClient is None:
            self.dynamodbClient = boto3.client('dynamodb',
                                               region_name=self.region_name,
                                               aws_access_key_id=self.aws_access_key_id,
                                               aws_secret_access_key=self.aws_secret_access_key)

    def initResource(self):
        if self.dynamodbResource is None:
            self.dynamodbResource = boto3.resource('dynamodb',
                                                   region_name=self.region_name,
                                                   aws_access_key_id=self.aws_access_key_id,
                                                   aws_secret_access_key=self.aws_secret_access_key)

    @staticmethod
    def idb(configuration):
        return AwsDynamoDb(configuration)

    def useAccount(self, accountName, accountCertificate=None):
        super().useAccount(accountName, accountCertificate)

    def createAccount(self, accountName, accountCertificate=None):
        self.useAccount(accountName, accountCertificate)
        metadata = self.getItem(self.attributesKeyNames['delete'], accountCertificate)
        if metadata:
            return False
        metadata = self.getItem(self.attributesKeyNames['metadata'], accountCertificate)
        if metadata:
            return False
        metadata = {}
        self.putItem(self.attributesKeyNames['metadata'], metadata, accountCertificate)
        metadata = self.getItem(self.attributesKeyNames['metadata'], accountCertificate)
        return (metadata is not None)

    def deleteAccount(self, accountName, accountCertificate=None):
        self.useAccount(accountName, accountCertificate)
        metadata = self.getItem(self.attributesKeyNames['metadata'], accountCertificate)
        if metadata:
            self.putItem(self.attributesKeyNames['delete'], metadata, accountCertificate)
            self.deleteItem(self.attributesKeyNames['metadata'], accountCertificate)
        else:
            return False
        metadata = self.getItem(self.attributesKeyNames['metadata'], accountCertificate)
        return (metadata is None)

    def backupAccount(self, accountName, backupConfiguration, accountCertificate=None):
        raise NotImplementedError("Not Implemented!")

    def exportAccount(self, accountName, exportConfiguration, accountCertificate=None):
        raise NotImplementedError("Not Implemented!")

    def createAccountMetadata(self, accountCertificate=None):
        metadata = {}
        return self.putItem(self.attributesKeyNames['metadata'], metadata, accountCertificate)

    def setAccountMetadata(self, metadata, accountCertificate=None):
        return self.updateItem(self.attributesKeyNames['metadata'], metadata, accountCertificate)

    def getAccountMetadata(self, accountCertificate=None):
        return self.getItem(self.attributesKeyNames['metadata'], accountCertificate)

    def deleteAccountMetadata(self, accountCertificate=None):
        return self.deleteItem(self.attributesKeyNames['metadata'], accountCertificate)

    def __putItem(self, idbId, data, options, idbCertificate=None):
        self.initResource()
        table = self.dynamodbResource.Table(self.configuration['table_name'])
        try:
            response = table.put_item(Item=
            {
                self.dbKeyNames['account']: self.accountName,
                self.dbKeyNames['id']: idbId,
                **data
            }, **options)
        except ClientError as e:
            response = (e.response['Error']['Message'])
        except:
            response = None
        return (response)

    def putItem(self, idbId, data, idbCertificate=None):
        storageEngine = IdbankStorageEngine(
            {
                'idbFormat': IdbankStorageFormat.IDB_FORMAT_V1,
                'idbType': IdbankStorageType.IDB_TYPE_BASE64
            })
        if not isinstance(data, str):
            data = json.dumps(data)
        data = storageEngine.convert(data)
        dataPut = {
            self.dbKeyNames['public']:
                {
                    self.attributesKeyNames['item']: data
                },
            # For initial release we do not store any protected and private data
            # self.dbKeyNames['protected']: None,
            # self.dbKeyNames['private']: None
        }
        options = {
            'ConditionExpression': 'attribute_not_exists(#id)',
            'ExpressionAttributeNames': {
                '#id': self.dbKeyNames['id']
            }
        }
        return self.__putItem(idbId, dataPut, options, idbCertificate)

    def __updateItem(self, idbId, data, idbCertificate=None):
        self.initResource()
        table = self.dynamodbResource.Table(self.configuration['table_name'])
        key = {self.dbKeyNames['account']: self.accountName, self.dbKeyNames['id']: idbId}
        try:
            response = table.update_item(
                Key=key,
                **data
            )
        except ClientError as e:
            response = (e.response['Error']['Message'])
        except:
            response = None
        return (response)

    def updateItem(self, idbId, data, idbCertificate=None):
        storageEngine = IdbankStorageEngine(
            {
                'idbFormat': IdbankStorageFormat.IDB_FORMAT_V1,
                'idbType': IdbankStorageType.IDB_TYPE_BASE64
            })
        if not isinstance(data, str):
            data = json.dumps(data)
        data = storageEngine.convert(data)
        dataUpdate = {
            'ConditionExpression': 'attribute_exists(#id)',
            'UpdateExpression': 'SET #idbpublicKey=:idbpublicValue',
            'ExpressionAttributeNames': {
                '#idbpublicKey': self.dbKeyNames['public'],
                '#id': self.dbKeyNames['id']
            },
            'ExpressionAttributeValues': {
                ':idbpublicValue': {
                    self.attributesKeyNames['item']: data
                }
            }
        }
        return self.__updateItem(idbId, dataUpdate, idbCertificate)

    def __getItem(self, idbId, idbCertificate=None):
        self.initResource()
        table = self.dynamodbResource.Table(self.configuration['table_name'])
        response = table.get_item(Key=
        {
            self.dbKeyNames['account']: self.accountName,
            self.dbKeyNames['id']: idbId,
        })
        if response is not None and \
                'Item' in response:
            item = response['Item']
        else:
            item = None
        return item

    def getItem(self, idbId, idbCertificate=None):
        item = self.__getItem(idbId, idbCertificate)
        if item is not None and \
                self.dbKeyNames['public'] in item and \
                self.attributesKeyNames['item'] in item[self.dbKeyNames['public']]:
            item = item[self.dbKeyNames['public']][self.attributesKeyNames['item']]
            if isinstance(item, str):
                storageEngine = IdbankStorageEngine()
                item = storageEngine.parse(item)
        else:
            item = None
        return item

    def deleteItem(self, idbId, idbCertificate=None):
        self.initResource()
        table = self.dynamodbResource.Table(self.configuration['table_name'])
        try:
            options = {
                'ConditionExpression': 'attribute_exists(#id)',
                'ExpressionAttributeNames': {
                    '#id': self.dbKeyNames['id']
                }
            }
            response = table.delete_item(Key=
            {
                self.dbKeyNames['account']: self.accountName,
                self.dbKeyNames['id']: idbId,
            }, **options)
        except ClientError as e:
            response = (e.response['Error']['Message'])
        except:
            response = None
        return (response)

        return response

    def countItems(self, query, idbCertificate=None):
        response = self.__findItems(query, True, idbCertificate)
        if response is not None and \
                'Count' in response:
            count = response['Count']
        else:
            count = None
        return count

    def countAllItems(self, idbCertificate=None):
        return self.countItems(None, idbCertificate)

    def findItems(self, query, idbCertificate=None):
        response = self.__findItems(query, False, idbCertificate)
        if response is not None:
            def filterItems(item):
                return item[self.dbKeyNames['id']] in [self.attributesKeyNames['metadata'],
                                                       self.attributesKeyNames['delete']]

            def formatItem(item):
                if item is not None and \
                        self.dbKeyNames['public'] in item and \
                        self.attributesKeyNames['item'] in item[self.dbKeyNames['public']]:
                    item = {
                        item[self.dbKeyNames['id']]: item[self.dbKeyNames['public']][self.attributesKeyNames['item']]}
                else:
                    item = None
                return item

            def formatIdbidItem(item):
                if item is not None and \
                        self.dbKeyNames['id'] in item:
                    item = item[self.dbKeyNames['id']]
                else:
                    item = None
                return item

            items = {'Items': [], 'Count': 0}
            count = 0
            if 'Items' in response:
                if query and 'ProjectionExpression' in query and query['ProjectionExpression']:
                    if query['ProjectionExpression'] == self.dbKeyNames['id']:
                        items['Items'] = [formatIdbidItem(item) for item in response['Items'] if not filterItems(item)]
                        count = len(items['Items'])
                    else:
                        items['Items'] = response['Items']
                        count = response['Count']
                else:
                    items['Items'] = [formatItem(item) for item in response['Items'] if not filterItems(item)]
                    count = len(items['Items'])
            if 'Count' in response:
                items['Count'] = count
            if 'LastEvaluatedKey' in response:
                items['LastEvaluatedKey'] = response['LastEvaluatedKey']
            items = json.dumps(items, cls=DecimalEncoder)
        else:
            items = None
        return items

    def __findItems(self, query, count=False, idbCertificate=None):
        self.initResource()
        table = self.dynamodbResource.Table(self.configuration['table_name'])
        if query:
            query = query.copy()
        else:
            query = {}
        dataFind = {
            'KeyConditionExpression': '(#account = :account)',
            'ExpressionAttributeNames': {
                '#account': self.dbKeyNames['account'],
            },
            'ExpressionAttributeValues': {
                ':account': self.accountName,
            },
            'ReturnConsumedCapacity': 'TOTAL',
        }

        if count:
            dataFind['Select'] = 'COUNT'
            if 'ProjectionExpression' in query:
                del query['ProjectionExpression']
        if 'ExpressionAttributeNames' in query and not query['ExpressionAttributeNames']:
            del query['ExpressionAttributeNames']
        if 'ExpressionAttributeValues' in query and not query['ExpressionAttributeValues']:
            del query['ExpressionAttributeValues']
        if 'FilterExpression' in query:
            if query['FilterExpression']:
                query['FilterExpression'] = query['FilterExpression'].replace('#idb#', 'idbpublic.idbitem')
            else:
                del query['FilterExpression']
        if 'ProjectionExpression' in query:
            if query['ProjectionExpression']:
                query['ProjectionExpression'] = query['ProjectionExpression'].replace('#idb#', 'idbpublic.idbitem')
            else:
                del query['ProjectionExpression']

        IdbCommon.dictionaryMerge(dataFind, query)
        response = table.query(**dataFind)
        return response

    def generateExclusiveStartKey(self, page):
        return {self.dbKeyNames['account']: self.accountName, self.dbKeyNames['id']: page}

################################################################################
#                                End of file                                   #
################################################################################
