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

from abc import abstractmethod


################################################################################
# Module                                                                       #
################################################################################

class IdbankStorageBase:
    configuration = None
    accountName = None
    accountCertificate = None

    def __init__(self, configuration):
        self.configuration = configuration

    def printConfiguration(self):
        print('configuration')
        print(self.configuration)
        print('accountName')
        print(self.accountName)
        print('accountCertificate')
        print(self.accountCertificate)

    @abstractmethod
    def useAccount(self, accountName, accountCertificate = None):
        self.accountName = accountName
        self.accountCertificate = accountCertificate

    @abstractmethod
    def createAccount(self, accountName, accountCertificate = None):
        pass

    @abstractmethod
    def deleteAccount(self, accountName, accountCertificate = None):
        pass

    @abstractmethod
    def backupAccount(self, accountName, backupConfiguration, accountCertificate = None):
        pass

    @abstractmethod
    def exportAccount(self, accountName, exportConfiguration, accountCertificate = None):
        pass

    # Item structure:
    # public: {not encrypted data}
    # protected: {encrypted uses IDB account certificate - the same cert for all account items}
    # private: {encrypted uses IDB user certificate}

    @abstractmethod
    def putItem(self, idbId, data, idbCertificate = None):
        pass

    @abstractmethod
    def updateItem(self, idbId, data, idbCertificate = None):
        pass

    @abstractmethod
    def getItem(self, idbId, idbCertificate = None):
        pass

    @abstractmethod
    def deleteItem(self, idbId, idbCertificate = None):
        pass

    @abstractmethod
    def findItem(self, query, idbCertificate = None):
        pass

################################################################################
#                                End of file                                   #
################################################################################
