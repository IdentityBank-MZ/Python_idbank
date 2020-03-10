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

from .IdbQueryResponse import IdbQueryResponse


################################################################################
# Module                                                                       #
################################################################################

class IdbQueryError:

    @staticmethod
    def requestError(requestId: str = None) -> str:
        return IdbQueryResponse.response(400,
                                         'Bad Request',
                                         None,
                                         requestId)

    @staticmethod
    def requestNotFound(requestId: str = None) -> str:
        return IdbQueryResponse.response(404,
                                         'Not Found',
                                         None,
                                         requestId)

    @staticmethod
    def requestUnsupported(requestId: str = None) -> str:
        return IdbQueryResponse.response(415,
                                         'Unsupported',
                                         None,
                                         requestId)

    @staticmethod
    def requestUnsupportedService(message=None, requestId: str = None) -> str:
        errorMessage = 'Unsupported service'
        if message and logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
            errorMessage = errorMessage + '! ' + message
        return IdbQueryResponse.response(455,
                                         errorMessage,
                                         None,
                                         requestId)

    @staticmethod
    def requestQueryError(message=None, requestId: str = None) -> str:
        errorMessage = 'Request error'
        if message and logging.getLevelName(logging.getLogger().getEffectiveLevel()) == 'DEBUG':
            errorMessage = errorMessage + '! ' + message
        return IdbQueryResponse.response(457,
                                         errorMessage,
                                         None,
                                         requestId)

    @staticmethod
    def requestInternalServerError(requestId: str = None) -> str:
        return IdbQueryResponse.response(500,
                                         'Internal Server Error',
                                         None,
                                         requestId)

    @staticmethod
    def requestNotImplemented(requestId: str = None) -> str:
        return IdbQueryResponse.response(501,
                                         'Not Implemented',
                                         None,
                                         requestId)

    @staticmethod
    def requestServiceUnavailable(requestId: str = None) -> str:
        return IdbQueryResponse.response(503,
                                         'Service Unavailable',
                                         None,
                                         requestId)

    @staticmethod
    def requestServiceTimeout(requestId: str = None) -> str:
        return IdbQueryResponse.response(504,
                                         'Service Timeout',
                                         None,
                                         requestId)

    def itemAlreadyExisting(requestId: str = None) -> str:
        return IdbQueryResponse.response(1510,
                                         'Already existing',
                                         None,
                                         requestId)

    def itemNotFound(requestId: str = None) -> str:
        return IdbQueryResponse.response(1511,
                                         'Item Not Found',
                                         None,
                                         requestId)

################################################################################
#                                End of file                                   #
################################################################################
