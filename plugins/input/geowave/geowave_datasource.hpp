#ifndef GEOWAVE_DATASOURCE_HPP
#define GEOWAVE_DATASOURCE_HPP

// mapnik
#include <mapnik/datasource.hpp>
#include <mapnik/params.hpp>
#include <mapnik/query.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/box2d.hpp>
#include <mapnik/coord.hpp>
#include <mapnik/feature_layer_desc.hpp>

// boost
#include <boost/optional.hpp>
#include <memory>

// stl
#include <string>

// GeoWave
#include "jace/proxy/mil/nga/giat/geowave/accumulo/BasicAccumuloOperations.h"
using jace::proxy::mil::nga::giat::geowave::accumulo::BasicAccumuloOperations;

class geowave_datasource : public mapnik::datasource
{
public:
    geowave_datasource(mapnik::parameters const& params);
    virtual ~geowave_datasource ();
    mapnik::datasource::datasource_t type() const;
    static const char * name();
    mapnik::featureset_ptr features(mapnik::query const& q) const;
    mapnik::featureset_ptr features_at_point(mapnik::coord2d const& pt, double tol = 0) const;
    mapnik::box2d<double> envelope() const;
    boost::optional<mapnik::datasource::geometry_t> get_geometry_type() const;
    mapnik::layer_descriptor get_descriptor() const;

private:
    void init(mapnik::parameters const& params);
    int createJvm();
    
    mapnik::layer_descriptor desc_;
    mapnik::box2d<double> extent_;
    std::string zookeeper_url_;
    std::string instance_name_;
    std::string username_;
    std::string password_;
    std::string table_namespace_;
    std::string adapter_id_;
    
    BasicAccumuloOperations accumulo_operations_;
};


#endif // GEOWAVE_DATASOURCE_HPP
