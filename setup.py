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

import os

from setuptools import setup

################################################################################
# Module                                                                       #
################################################################################

description = 'IDBank - Identity Bank data driver.'


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


try:
    long_description = read('README.md')
except IOError:
    long_description = description

setup(
    name='idbank',
    version='0.1',
    description=description,
    long_description=long_description,
    keywords="identity idb bank secure storage encryption decryption share tools",
    author='Marcin Zelek',
    author_email='marcin.zelek@identitybank.eu',
    license='GNU Affero General Public License v3.0',
    url='https://www.identitybank.eu',
    packages=['idbank',
              'idbank.idbankcommon',
              'idbank.idbankquery',
              'idbank.idbankstorage',
              'idbank.idbankhelper'],
    entry_points=
    {
        'console_scripts':
        [
            'idbankclient = idbank.idbankclient:main',
            'idbankserver = idbank.idbankserver:main',
        ],
    },
    zip_safe=False
)

################################################################################
#                                End of file                                   #
################################################################################
