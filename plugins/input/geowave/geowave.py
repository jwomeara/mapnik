#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#
#
# This file is part of Mapnik (c++ mapping toolkit)
# Copyright (C) 2005 Jean-Francois Doyon
#
# Mapnik is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
import mapnik
mapnik.logger.set_severity(mapnik.severity_type.Debug)

m = mapnik.Map(600,300) # create a map with a given width and height in pixels
# note: m.srs will default to '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
# the 'map.srs' is the target projection of the map and can be whatever you wish 
m.background = mapnik.Color('steelblue') # set background colour to 'steelblue'.  

s = mapnik.Style() # style object to hold rules
r = mapnik.Rule() # rule object to hold symbolizers
# to fill a polygon we create a PolygonSymbolizer
polygon_symbolizer = mapnik.PolygonSymbolizer()
polygon_symbolizer.fill = mapnik.Color('#f2eff9')
r.symbols.append(polygon_symbolizer) # add the symbolizer to the rule object
# to add outlines to a polygon we create a LineSymbolizer
line_symbolizer = mapnik.LineSymbolizer()
line_symbolizer.stroke = mapnik.Color('rgb(50%,50%,50%)')
line_symbolizer.stroke_width = 0.1
r.symbols.append(line_symbolizer) # add the symbolizer to the rule object
s.rules.append(r) # now add the rule to the style and we're done

m.append_style('My Style',s) # Styles are given names only as they are applied to the map

ds = mapnik.Datasource(type='geowave',zookeeper_url='localhost:2181',instance_name='geowave',username='root',password='password',table_namespace='mapnik',adapter_id='TM_WORLD_BORDERS_SIMPL-0.3')
extent = extent = mapnik.Box2d(-180.0, -90.0, 180.0, 90.0)

layer = mapnik.Layer('world')
layer.datasource = ds
layer.styles.append('My Style')

m.layers.append(layer)
m.zoom_to_box(extent)

# Write the data to a png image called world.png the current directory
mapnik.render_to_file(m,'world.png', 'png')

# Exit the Python interpreter
exit() # or ctrl-d

