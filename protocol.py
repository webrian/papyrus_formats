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

__author__ = "Adrian Weber, Centre for Development and Environment, University of Bern"
__date__ = "$Apr 29, 2013 6:55:21 AM$"

from geoalchemy import functions
import geojson
import logging
from papyrus.protocol import *
import rpy2.rinterface as rinterface
import rpy2.robjects as robjects
import shapefile
from shapely.wkb import loads
import simplejson as json
from sqlalchemy import func
from sqlalchemy import or_
from tempfile import NamedTemporaryFile
try:
    from StringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile
import xlwt

log = logging.getLogger(__name__)

# Map of EPSG codes to write the .prj files
# taken from spatialreference.org
epsg_code = {
4326: 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]',
32648: 'PROJCS["WGS_1984_UTM_Zone_48N",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",105],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["Meter",1]]'
}

def logical_attr_filter(request, mapped_class):
    """
    Create an SQLAlchemy filter (a ClauseList object) based
    on the request params (``queryable``, ``eq``, ``ne``, ...).

    This function extend the mapfish protocol with the possibility
    to create logical OR or AND or NOT (not exposed to the GUI) queries.

    Now it's possible to create attribute queries like:
    <ul><li>did=102,103</li></ul>
    and the new "filter" parameter
    <ul><li>did=102,103&filter=OR&</li></ul>

    @param request The server request with according parameters
    @param mapped_class
    @return Returns a SQLAlchemy filter according to the request
    """

    mapping = {
	'eq': '__eq__',
	'ne': '__ne__',
	'lt': '__lt__',
	'lte': '__le__',
	'gt': '__gt__',
	'gte': '__ge__',
	'like': 'like',
	'ilike': 'ilike'
    }

    filters = []
    if 'queryable' in request.params:
	queryable = request.params['queryable'].split(',')
	for k in request.params:
	    if len(request.params[k]) <= 0 or '__' not in k:
		continue

	    col, op = k.split("__")

	    if col not in queryable or op not in mapping.keys():
		continue

	    values = request.params[k].split(",")

	    for v in values:
	        column = getattr(mapped_class, col)
		f = getattr(column, mapping[op])(v)
                filters.append(f)

    if 'logical_op' in request.params:
        logicalOp = request.params['logical_op']
        if logicalOp.lower() == 'and':
	    return and_(*filters) if len(filters) > 0 else None
        if logicalOp.lower() == 'or':
	    return or_(*filters) if len(filters) > 0 else None
        if logicalOp.lower() == 'not':
	    return not_(or_(*filters)) if len(filters) > 0 else None

    return or_(*filters) if len(filters) > 0 else None

class FormatsProtocol(Protocol):

    def _query(self, request, filter=None, execute=True):
        """ Build a query based on the filter and the request params,
            and send the query to the database. """
        limit = None
        offset = None
        if 'maxfeatures' in request.params:
            limit = int(request.params['maxfeatures'])
        if 'limit' in request.params:
            limit = int(request.params['limit'])
        if 'offset' in request.params:
            offset = int(request.params['offset'])
        if filter is None:
            filter = create_filter(request, self.mapped_class, self.geom_attr)
        query = self.Session().query(self.mapped_class).filter(filter)
        order_by = self._get_order_by(request)
        if order_by is not None:
            query = query.order_by(order_by)
        query = query.limit(limit).offset(offset)
        if execute:
            return query.all()
        else:
            return query

    def read(self, request, filter=None, id=None, format='geojson', ** kwargs):
        """
        Build a query based on the filter or the idenfier, send the query
        to the database, and return a Feature or a FeatureCollection.
        """

        if format == 'geojson':

            ret = None
            if id is not None:
                o = self.Session.query(self.mapped_class).get(id)
                if o is None:
                    abort(404)
                ret = self._filter_attrs(o.__geo_interface__, request)
            else:
                objs = self._query(request, filter)
                ret = FeatureCollection(
                                        [self._filter_attrs(o.__geo_interface__, request) \
                                        for o in objs])

            return geojson.dumps(ret)

        if format == 'ext':
            if id is not None:
                query = self.Session.query(self.mapped_class).get(id)
            else:
                query = self._query(request, filter, False)
            return self._read_ext(request, query, filter=filter, name_mapping=kwargs['name_mapping'])

        if format == 'hist':
            if filter is None:
                filter = create_default_filter(request, self.mapped_class)
            return self._plot_histogram(request, self.Session.query(self.mapped_class).filter(filter), kwargs['categories'])

        if format == 'xls':

            metadata = kwargs.get("metadata", None)

            query = self._query(request, filter, False)
            return self._read_xls(request, query, filter=filter, metadata=metadata)

        if format == 'shp':

            epsg = kwargs.get("epsg", 4326)

            metadata = kwargs.get("metadata", None)

            if filter is None:
                filter = create_filter(request, self.mapped_class, 'wkb_geometry')

            mapped_attributes = []
            mapped_attributes.append(functions.wkb(functions.transform(getattr(self.mapped_class, 'wkb_geometry'), epsg)).label("geometry_column"))
            for attr in request.params.get("attrs").split(","):
                mapped_attributes.append(getattr(self.mapped_class, attr))

            return self._read_shp(request, self.Session.query(* mapped_attributes).filter(filter), epsg=epsg, metadata=metadata)

    def _read_ext(self, request, query, filter=None, name_mapping=None):
        """
        A format suitable for Ext json stores.
        """

        output = {}
        output['totalResults'] = self.count(request, filter)

        attrs = request.params['attrs'].split(',')

        output['metaData'] = {'totalProperty': 'totalResults', 'root': 'rows'}
        output['metaData']['fields'] = []

        output['rows'] = []

        for i in query.all():
            row = {}
            for k in attrs:
                value = getattr(i, k)
                if name_mapping is not None and k in name_mapping:
                    k = name_mapping[k]

                row[k] = value


                # Check if metadata of current attribute is already declared
                is_declared = False
                for f in output['metaData']['fields']:
                    try:
                        if f['name'] == k:
                            is_declared = True
                    except TypeError:
                        pass

                if not is_declared:
                    metadataField = {}
                    metadataField['name'] = k
                    if isinstance(value, (float)):
                        metadataField['type'] = 'float'
                    elif isinstance(value, (int)):
                        metadataField['type'] = 'int'
                    else:
                        metadataField['type'] = 'string'
                    output['metaData']['fields'].append(metadataField)

            # Append the current row to the rows
            output['rows'].append(row)

        return json.dumps(output)


    def _plot_histogram(self, request, query, categories=None):

        # Get the requested attribute
        attr = request.params.get('attrs').split(",")[0]

        defaultSide = 480

        try:
            height = int(request.params.get("height", defaultSide))
        except ValueError:
            height = defaultSide
        try:
            width = int(request.params.get("width", defaultSide))
        except ValueError:
            width = defaultSide

        rbreaks = 10
        if 'breaks' in request.params:
            rbreaks = int(request.params['breaks'])

        rinterface.initr()

        r = robjects.r
        r.library('grDevices')

        mappedAttribute = getattr(self.mapped_class, attr)

        # Create a temporary file
        file = NamedTemporaryFile()
        r.png(file.name, width=width, height=height)
        r.par(bg="#F0F0F0", mar=robjects.FloatVector([2.6, 4.1, 3.1, 1.1]))

        bar_color = "#BEBEBE"

        # If categories is not none, then the current attribute has categories and
        # we want to draw a barplot instead of a histogram
        if categories is not None:

            names = []
            v = []
            #for i in self.Session.query(func.count(mappedAttribute)).filter(mappedAttribute.in_(keys)).group_by(mappedAttribute):
            for a, count in query.from_self(mappedAttribute, func.count(mappedAttribute)).filter(mappedAttribute.in_(categories.keys())).group_by(mappedAttribute):
                v.append(int(count))
                names.append(categories[unicode(a)].encode('UTF-8'))
            x = robjects.IntVector(v)

            x.names = robjects.StrVector(names)
            r.barplot(x, col=bar_color, xlab=str(), ylab=str(), main=str(), ** {"names.arg": robjects.StrVector(names)})
            #r.par(bg="#F0F0F0", mar=robjects.FloatVector([1.5, 1.5, 1.5, 1.5]))
            #r.pie(x, labels=robjects.StrVector(names), clockwise=True)

        # Handle quantitative data
        else:

            v = []
            for i in query.all():
                v.append(getattr(i, attr))

            x = robjects.FloatVector(v)

            r.hist(x, col=bar_color, breaks=rbreaks, ylab=str(), xlab=str(), main=str())

        # Finish drawing
        r('dev.off()')

        f = open(file.name, 'r')

        return f

    def _read_xls(self, request, query, ** kwargs):

        requested_attrs = request.params.get("attrs").split(",")

        workbook = xlwt.Workbook(encoding='utf-8')
        sheet = workbook.add_sheet("data")
        
        row = 0
        column = 0
        for a in requested_attrs:
            sheet.write(row, column, a, xlwt.easyxf('font: bold true; borders: bottom THIN;'))
            column += 1

        row += 1
        
        for i in query.all():
            column = 0
            for a in requested_attrs:
                sheet.write(row, column, getattr(i, a))
                column += 1

            row += 1
        
        if kwargs.get("metadata", None) is not None:

            self._write_metadata(workbook, kwargs.get("metadata"))
            # Write the workbook to a file-like object
            xls = StringIO()
            # Save the workbook to the memory object
            workbook.save(xls)

        # Create a file-like object
        s = StringIO()
        # Save the workbook to the memory object
        workbook.save(s)
        return s

    def _read_shp(self, request, query, ** kwargs):

        requested_attrs = request.params.get("attrs").split(",")

        # Get the first feature to guess the datatype
        first_record = query.first()

        # Create geometry from AsBinary query
        first_geom = loads(str(getattr(first_record, 'geometry_column')))

        log.debug("Geometry type is %s" % first_geom.geom_type)

        w = shapefile.Writer(shapefile.POLYGON)
        if first_geom.geom_type == "Point":
            w = shapefile.Writer(shapefile.POINT)
        elif first_geom.geom_type == "LineString":
            w = shapefile.Writer(shapefile.POLYLINE)

        w.autoBalance = 1

        # Loop all requested attributes
        for attr in requested_attrs:

            first_value = getattr(first_record, attr)

            # Guess the datatype
            if isinstance(first_value, int):
                w.field(str(attr), 'N', 40)
            elif isinstance(first_value, float):
                w.field(str(attr), 'N', 40, 10)
            else:
                w.field(str(attr), 'C', 40)

        # Now query all features
        for i in query.all():

            # Create geometry from self.mapped_class
            #g = loads(str(getattr(i, 'geometry_column').geom_wkb))
            # Create geometry from AsBinary query
            g = loads(str(getattr(i, 'geometry_column')))

            # Handle point geometries
            if g.geom_type == "Point":

                w.point(g.coords[0][0], g.coords[0][1])

            # Handle linestring geometries
            if g.geom_type == "LineString":

                point_list = []

                for p in g.coords:
                    point_list.append([p[0], p[1]])

                w.line(parts=[point_list])

            # Handle polygon geometries
            if g.geom_type == "Polygon":

                ring_list = []

                point_list = []

                for j in g.exterior.coords:
                    point_list.append([j[0], j[1]])

                ring_list.append(point_list)

                for interior in g.interiors:

                    point_list = []

                    for k in interior.coords:
                        point_list.append([k[0], k[1]])

                    ring_list.append(point_list)

                w.poly(shapeType=shapefile.POLYGON, parts=ring_list)

            values = []
            for v in requested_attrs:
                try:
                    values.append(str(getattr(i, v)))
                except UnicodeEncodeError:
                    values.append(str(getattr(i, v).encode("UTF-8")))

            w.record(* values)

        # Create the required files and fill them
        shp = StringIO()
        shx = StringIO()
        dbf = StringIO()
        cpg = StringIO()
        prj = StringIO()
        w.saveShp(shp)
        w.saveShx(shx)
        w.saveDbf(dbf)
        cpg.write("UTF-8")
        prj.write(epsg_code[kwargs.get("epsg", 4326)])

        # Create a memory file-like deflated zip file
        s = StringIO()
        f = ZipFile(s, 'w', ZIP_DEFLATED)
        f.writestr("data.shp", shp.getvalue())
        f.writestr("data.dbf", dbf.getvalue())
        f.writestr("data.shx", shx.getvalue())
        f.writestr("data.cpg", cpg.getvalue())
        f.writestr("data.prj", prj.getvalue())


        if kwargs.get("metadata") is not None:
            wb = xlwt.Workbook(encoding='utf-8')

            self._write_metadata(wb, kwargs.get("metadata"))
            # Write the workbook to a file-like object
            xls = StringIO()
            # Save the workbook to the memory object
            wb.save(xls)

            f.writestr("metadata.xls", xls.getvalue())

        # Close the zip file
        f.close()

        # And return the content
        return s

    def _write_metadata(self, workbook, metadata):

        sheet = workbook.add_sheet("metadata")

        row = 0

        # Create a style that draws a bottom line
        bottomMediumStlye = xlwt.easyxf('font: bold true; borders: bottom THIN;')

        # Write the column headers
        column = 0
        for h in metadata.get_headers():
            sheet.write(row, column, h, bottomMediumStlye)
            column += 1
            
        row += 1

        # Write the variable metadata
        for r in metadata.get_rows():
            column = 0
            for c in r:
                sheet.write(row, column, c)
                column += 1

            row += 1

        # Write contact address
        if metadata.get_address() is not None:
            # One row as space
            row += 1
            sheet.write(row, 0, "*************************************************************")
            row += 1
            sheet.write(row, 0, "*")
            sheet.write(row, 1, "Points of Contact", xlwt.easyxf('font: bold true;'))
            row += 1
            sheet.write(row, 0, "*************************************************************")

        for a in metadata.get_address():
            row += 1
            column = 0
            for c in a:
                sheet.write(row, 0, "*")
                sheet.write(row, 1, c)
                row += 1
            sheet.write(row, 0, "*************************************************************")