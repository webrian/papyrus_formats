#
# mapnik_formats
# Copyright (C) 2013 Centre for Development and Environment, University of Bern
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

__author__="Adrian Weber, Centre for Development and Environment, University of Bern"
__date__ ="$Apr 30, 2013 5:21:48 PM$"

class Metadata(object):

    def __init__(self, headers=[], rows=[], address=[]):

        self._headers = headers
        self._rows = rows
        self._address = address

    def get_headers(self):
        return self._headers

    def get_rows(self):
        return self._rows

    def get_address(self):
        return self._address

    