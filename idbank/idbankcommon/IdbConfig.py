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

import jsonsimpleconfig


################################################################################
# Module                                                                       #
################################################################################

class IdbConfig:

    @staticmethod
    def getConfig(jscConfigFilePath: str, connectionName: str) -> dict:
        configuration = jsonsimpleconfig.load(jscConfigFilePath)
        if not configuration:
            raise ValueError("Wrong configuration file!")

        if configuration:
            section = '"IDB"."{}"'.format(connectionName)
            connectionType = configuration.getValue(section, 'type')
            if connectionType:
                if connectionType == 'IdentityBank.V1':
                    idbConfiguration = IdbConfig.getIdentityBankV1Config(section, configuration)
                    if idbConfiguration:
                        idbConfiguration['connectionType'] = connectionType
                        server = IdbConfig.getServerConfig(section, configuration)
                        if server:
                            idbConfiguration['server'] = server
                        security = IdbConfig.getServerSecurityConfig(section, configuration)
                        if security:
                            idbConfiguration['Security'] = security
                        firewall = IdbConfig.getServerFirewallConfig(section, configuration)
                        if firewall:
                            idbConfiguration['Firewall'] = firewall
                        return idbConfiguration
                else:
                    print("The IDB connection type '{}' is not supported.".format(connectionType))
            else:
                print("Error parsing configuration file for connection name: {}. Execution interrupted ...".format(
                    connectionName))

        return None

    @staticmethod
    def getServerConfig(section: str, configuration: jsonsimpleconfig.jscdata.JscData) -> dict:
        serverSection = '{}."server"."bind"'.format(section)
        serverSectionData = configuration.getSection(serverSection)
        if serverSectionData:
            return serverSectionData
        return None

    @staticmethod
    def getServerSecurityConfig(section: str, configuration: jsonsimpleconfig.jscdata.JscData) -> dict:
        securitySection = '{}."server"."Security"'.format(section)
        securitySectionData = configuration.getSection(securitySection)
        if securitySectionData:
            return securitySectionData
        return None

    @staticmethod
    def getServerFirewallConfig(section: str, configuration: jsonsimpleconfig.jscdata.JscData) -> dict:
        firewallSection = '{}."server"."Security"."Firewall"'.format(section)
        firewallSectionData = configuration.getSection(firewallSection)
        if firewallSectionData:
            return firewallSectionData
        return None

    @staticmethod
    def getIdentityBankV1Config(section: str, configuration: jsonsimpleconfig.jscdata.JscData) -> dict:
        sectionConnectionBusinessData = '{}."configuration"."connection"."business"."data"'.format(section)
        sectionConnectionBusiness = '{}."configuration"."connection"."business"'.format(section)
        sectionConnectionPeople = '{}."configuration"."connection"."people"'.format(section)
        sectionConnectionRelation = '{}."configuration"."connection"."relation"'.format(section)
        configuration = {
            'connectionBusinessData': configuration.getSection(sectionConnectionBusinessData),
            'connectionBusiness': configuration.getSection(sectionConnectionBusiness),
            'connectionPeople': configuration.getSection(sectionConnectionPeople),
            'connectionRelation': configuration.getSection(sectionConnectionRelation),
        }
        return configuration

################################################################################
#                                End of file                                   #
################################################################################
