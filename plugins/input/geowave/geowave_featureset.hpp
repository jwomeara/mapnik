#ifndef GEOWAVE_FEATURESET_HPP
#define GEOWAVE_FEATURESET_HPP

// mapnik
#include <mapnik/datasource.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/geometry.hpp>

// boost

// jace
#include "jace/Namespace.h"

// GeoWave
#include "jace/proxy/mil/nga/giat/geowave/store/CloseableIterator.h"
using jace::proxy::mil::nga::giat::geowave::store::CloseableIterator;

#include "jace/proxy/com/vividsolutions/jts/geom/LineString.h"
using jace::proxy::com::vividsolutions::jts::geom::LineString;
#include "jace/proxy/com/vividsolutions/jts/geom/Point.h"
using jace::proxy::com::vividsolutions::jts::geom::Point;

class geowave_featureset : public mapnik::Featureset
{
public:
    // this constructor can have any arguments you need
    geowave_featureset(CloseableIterator iterator);

    // desctructor
    virtual ~geowave_featureset();

    // mandatory: you must expose a next() method, called when rendering
    mapnik::feature_ptr next();

protected:

    // this method is used to convert a JTS LineString into a Mapnik Polygon
    void create_polygon(
                LineString line_string_in, 
                mapnik::geometry_type * poly_out);
    
    // this method is used to convert a JTS LineString into a Mapnik LineString
    void create_line_string(
                LineString line_string_in, 
                mapnik::geometry_type * line_string_out);
    
    // this method is used to convert a JTS Point to a Mapnik Point
    void create_point(
                Point point_in, 
                mapnik::geometry_type * point_out);

private:
    // members are up to you, but these are recommended
    mapnik::context_ptr ctx_;
    
    CloseableIterator iterator_;
    mapnik::value_integer feature_id_;
};

#endif // GEOWAVE_FEATURESET_HPP
