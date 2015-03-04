// plugin
#include "geowave_featureset.hpp"

// mapnik
#include <mapnik/feature_factory.hpp>
#include <mapnik/value_types.hpp>
#include <mapnik/debug.hpp>

// boost

// GeoWave
#include <jace/Jace.h>
using jace::java_cast;
using jace::java_new;
using jace::instanceof;

#include "jace/proxy/types/JDouble.h"
using jace::proxy::types::JDouble;
#include "jace/proxy/types/JFloat.h"
using jace::proxy::types::JFloat;
#include "jace/proxy/types/JInt.h"
using jace::proxy::types::JInt;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;
#include "jace/proxy/types/JShort.h"
using jace::proxy::types::JShort;

#include "jace/proxy/java/lang/Object.h"
using jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/Class.h"
using jace::proxy::java::lang::Class;
#include "jace/proxy/java/lang/Number.h"
using jace::proxy::java::lang::Number;
#include "jace/proxy/java/lang/Double.h"
using jace::proxy::java::lang::Double;
#include "jace/proxy/java/lang/Float.h"
using jace::proxy::java::lang::Float;
#include "jace/proxy/java/lang/Integer.h"
using jace::proxy::java::lang::Integer;
#include "jace/proxy/java/lang/Long.h"
using jace::proxy::java::lang::Long;
#include "jace/proxy/java/lang/Short.h"
using jace::proxy::java::lang::Short;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/util/Date.h"
using jace::proxy::java::util::Date;
#include "jace/proxy/java/util/List.h"
using jace::proxy::java::util::List;

#include "jace/proxy/com/vividsolutions/jts/geom/Coordinate.h"
using jace::proxy::com::vividsolutions::jts::geom::Coordinate;
#include "jace/proxy/com/vividsolutions/jts/geom/Geometry.h"
using jace::proxy::com::vividsolutions::jts::geom::Geometry;
#include "jace/proxy/com/vividsolutions/jts/geom/MultiLineString.h"
using jace::proxy::com::vividsolutions::jts::geom::MultiLineString;
#include "jace/proxy/com/vividsolutions/jts/geom/MultiPoint.h"
using jace::proxy::com::vividsolutions::jts::geom::MultiPoint;
#include "jace/proxy/com/vividsolutions/jts/geom/MultiPolygon.h"
using jace::proxy::com::vividsolutions::jts::geom::MultiPolygon;
#include "jace/proxy/com/vividsolutions/jts/geom/Polygon.h"
using jace::proxy::com::vividsolutions::jts::geom::Polygon;

#include "jace/proxy/org/opengis/feature/simple/SimpleFeature.h"
using jace::proxy::org::opengis::feature::simple::SimpleFeature;
#include "jace/proxy/org/opengis/feature/simple/SimpleFeatureType.h"
using jace::proxy::org::opengis::feature::simple::SimpleFeatureType;
#include "jace/proxy/org/opengis/feature/type/AttributeType.h"
using jace::proxy::org::opengis::feature::type::AttributeType;
#include "jace/proxy/org/opengis/feature/type/Name.h"
using jace::proxy::org::opengis::feature::type::Name;
#include "jace/proxy/org/opengis/feature/type/AttributeDescriptor.h"
using jace::proxy::org::opengis::feature::type::AttributeDescriptor;

geowave_featureset::geowave_featureset(CloseableIterator iterator) 
 : ctx_ (std::make_shared<mapnik::context_type>()),
   iterator_ (iterator),
   feature_id_ (1) { }

geowave_featureset::~geowave_featureset() {
    if (!iterator_.isNull())
        iterator_.close();
 }

void geowave_featureset::create_polygon(
                LineString line_string_in, 
                mapnik::geometry_type * poly_out)
{
    int numPts = (line_string_in.isClosed()) ? (int)line_string_in.getNumPoints() : line_string_in.getNumPoints()-1;
    for (int ptIdx = 0; ptIdx < numPts; ++ptIdx){
        Coordinate coord = line_string_in.getPointN(ptIdx).getCoordinate();
        if (ptIdx == 0)
            poly_out->move_to(coord.x(),coord.y());
        else
            poly_out->line_to(coord.x(),coord.y());
    }
    poly_out->close_path();
}

void geowave_featureset::create_line_string(
                LineString line_string_in, 
                mapnik::geometry_type * line_string_out)
{
    for (int ptIdx = 0; ptIdx < line_string_in.getNumPoints(); ++ptIdx){
        Coordinate coord = line_string_in.getPointN(ptIdx).getCoordinate();
        if (ptIdx == 0)
            line_string_out->move_to(coord.x(),coord.y());
        else
            line_string_out->line_to(coord.x(),coord.y());
    }
}

void geowave_featureset::create_point(
                Point point_in, 
                mapnik::geometry_type * point_out)
{
    Coordinate coord = point_in.getCoordinate();
    point_out->move_to(coord.x(), coord.y());
}
 
mapnik::feature_ptr geowave_featureset::next()
{
    MAPNIK_LOG_DEBUG(geowave) << "geowave_featureset::next";
    if (iterator_.hasNext())
    {
        // first, read a feature
        SimpleFeature simpleFeature = java_cast<SimpleFeature>(iterator_.next());
        List attribs = simpleFeature.getType().getAttributeDescriptors();

        MAPNIK_LOG_DEBUG(geowave) << "  feature #: " << feature_id_;

        MAPNIK_LOG_DEBUG(geowave) << "  feature id: " << simpleFeature.getID();

        // get the list of attributes for this feature
        // the featureset context needs to know the field schema
        // TODO: only do this for the first feature?
        if (feature_id_ == 1)
            for (int i = 0; i < attribs.size(); ++i)
                ctx_->push(java_cast<AttributeDescriptor>(attribs.get(i)).getLocalName());
       
        // create a new mapnik feature
        mapnik::feature_ptr feature(mapnik::feature_factory::create(ctx_, feature_id_++));
        
        // build the mapnik feature using geotools feature attributes
        for (int i = 0; i < attribs.size(); ++i){
            String name = java_cast<AttributeDescriptor>(attribs.get(
                    i)).getLocalName();
            Object attrib = simpleFeature.getAttribute(name);

            MAPNIK_LOG_DEBUG(geowave) << "  Attribute # " << i << " [" << name << "]";

            if (attrib.isNull()){
                MAPNIK_LOG_DEBUG(geowave) << "  Null Value";
                continue;
            }

            if (instanceof<Geometry>(attrib)) {
                MAPNIK_LOG_DEBUG(geowave) << "    Geometry";
                if (instanceof<Polygon>(attrib)) {
                    MAPNIK_LOG_DEBUG(geowave) << "    Polygon";
                    Polygon poly = java_cast<Polygon>(attrib);
                    
                    // handle exterior ring
                    mapnik::geometry_type * extPoly = new mapnik::geometry_type(mapnik::geometry_type::types::PolygonExterior);
                    create_polygon(poly.getExteriorRing(), extPoly);
                    feature->add_geometry(extPoly);
                    
                    // handle interior rings
                    for (int rngIdx = 0; rngIdx < poly.getNumInteriorRing(); ++rngIdx){
                        mapnik::geometry_type * intPoly = new mapnik::geometry_type(mapnik::geometry_type::types::PolygonInterior);
                        create_polygon(poly.getInteriorRingN(rngIdx), intPoly);
                        feature->add_geometry(intPoly);
                    }
                }
                else if (instanceof<MultiPolygon>(attrib)) {
                    MultiPolygon multiPoly = java_cast<MultiPolygon>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    MultiPolygon [" << multiPoly.getNumGeometries() << "]";
                    for (int geomIdx = 0; geomIdx < multiPoly.getNumGeometries(); ++geomIdx){
                        Polygon poly = java_cast<Polygon>(multiPoly.getGeometryN(geomIdx));
                        
                        // handle exterior ring
                        mapnik::geometry_type * extPoly = new mapnik::geometry_type(mapnik::geometry_type::types::PolygonExterior);
                        create_polygon(poly.getExteriorRing(), extPoly);
                        feature->add_geometry(extPoly);
                        
                        // handle interior rings
                        for (int rngIdx = 0; rngIdx < poly.getNumInteriorRing(); ++rngIdx){
                            mapnik::geometry_type * intPoly = new mapnik::geometry_type(mapnik::geometry_type::types::PolygonInterior);
                            create_polygon(poly.getInteriorRingN(rngIdx), intPoly);
                            feature->add_geometry(intPoly);
                        }
                    }
                }
                else if (instanceof<LineString>(attrib)) {
                    MAPNIK_LOG_DEBUG(geowave) << "    LineString";
                    mapnik::geometry_type * line = new mapnik::geometry_type(mapnik::geometry_type::types::LineString);
                    create_line_string(java_cast<LineString>(attrib), line);
                    feature->add_geometry(line);
                }
                else if (instanceof<MultiLineString>(attrib)) {
                    MultiLineString multiLineStr = java_cast<MultiLineString>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    MultiLineString [" << multiLineStr.getNumGeometries() << "]";
                    for (int geomIdx = 0; geomIdx < multiLineStr.getNumGeometries(); ++geomIdx){
                        mapnik::geometry_type * line = new mapnik::geometry_type(mapnik::geometry_type::types::LineString);
                        create_line_string(java_cast<LineString>(multiLineStr.getGeometryN(geomIdx)), line);
                        feature->add_geometry(line);
                    }
                }
                else if (instanceof<Point>(attrib)) {
                    MAPNIK_LOG_DEBUG(geowave) << "    Point";
                    mapnik::geometry_type * pt = new mapnik::geometry_type(mapnik::geometry_type::types::Point);
                    create_point(java_cast<Point>(attrib), pt);
                    feature->add_geometry(pt);
                }
                else if (instanceof<MultiPoint>(attrib)) {
                    MultiPoint multiPoint = java_cast<MultiPoint>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    MultiPoint  [" << multiPoint.getNumGeometries() << "]";
                    for (int geomIdx = 0; geomIdx < multiPoint.getNumGeometries(); ++geomIdx){
                        mapnik::geometry_type * pt = new mapnik::geometry_type(mapnik::geometry_type::types::Point);
                        create_point(java_cast<Point>(multiPoint.getGeometryN(geomIdx)), pt);
                        feature->add_geometry(pt);
                    }
                }
                else {
                    MAPNIK_LOG_DEBUG(geowave) << "Ruh-roh!!  Unknown Geometry type!";
                }
            }
            else if (instanceof<Number>(attrib)) {
                MAPNIK_LOG_DEBUG(geowave) << "    Number";
                if (instanceof<Double>(attrib)) {
                    Double value = java_cast<Double>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    Double [" << value.doubleValue() << "]";
                    feature->put(name, value.doubleValue());
                }
                else if (instanceof<Float>(attrib)) {
                    Float value = java_cast<Float>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    Float [" << value.floatValue() << "]";
                    feature->put(name, value.floatValue());
                }
                else if (instanceof<Integer>(attrib)) {
                    Integer value = java_cast<Integer>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    Integer [" << value.intValue() << "]"; 
                    feature->put(name, value.intValue());
                }
                else if (instanceof<Long>(attrib)) {
                    Long value = java_cast<Long>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    Long [" << value.longValue() << "]";
                    feature->put(name, value.longValue());
                }
                else if (instanceof<Short>(attrib)) {
                    MAPNIK_LOG_DEBUG(geowave) << "    Short";
                    Short value = java_cast<Short>(attrib);
                    MAPNIK_LOG_DEBUG(geowave) << "    Short [" << value.shortValue() << "]";
                    feature->put(name, value.shortValue());
                }
                else {
                    MAPNIK_LOG_DEBUG(geowave) << "Ruh-roh!!  Unknown number type!";
                }
            }
            else if (instanceof<String>(attrib)) {
                String value = java_cast<String>(attrib);
                MAPNIK_LOG_DEBUG(geowave) << "    String  [" << value << "]";
                feature->put(name, value);
            }
            else if (instanceof<Date>(attrib)) {
                Date value = java_cast<Date>(attrib);
                MAPNIK_LOG_DEBUG(geowave) << "    Date [" << value.toString() << "]";
                feature->put(name, value.toString());
            }
            else {
                MAPNIK_LOG_DEBUG(geowave) << "Unknown Attribute: Name[" << name << "]";
                MAPNIK_LOG_DEBUG(geowave) << "Unknown Attribute: Type[" << java_cast<AttributeDescriptor>(attribs.get(i)).getType().getBinding().toString() << "]";
            }
        }

        // return the feature!
        return feature;
    }

    // otherwise return an empty feature
    return mapnik::feature_ptr();
}
