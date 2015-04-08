// plugin
#include "geowave_datasource.hpp"
#include "geowave_featureset.hpp"

// mapnik
#include <mapnik/debug.hpp>

// jace
#include <jace/Jace.h>
using jace::java_cast;
using jace::java_new;
using jace::instanceof;

#include "jace/JNIException.h"
using jace::JNIException;

#include "jace/VirtualMachineShutdownError.h"
using jace::VirtualMachineShutdownError;

#include "jace/OptionList.h"
using jace::OptionList;
using jace::Option;
using jace::ClassPath;
using jace::Verbose;
using jace::CustomOption;

#include <jace/StaticVmLoader.h>
using jace::StaticVmLoader;

#ifdef _WIN32
#include "jace/Win32VmLoader.h"
using jace::Win32VmLoader;
const std::string os_pathsep(";");
#else
#include "jace/UnixVmLoader.h"
using ::jace::UnixVmLoader;
const std::string os_pathsep(":");
#endif

// GeoWave
#include "jace/JArray.h"
using jace::JArray;

#include "jace/proxy/types/JDouble.h"
using jace::proxy::types::JDouble;

#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

#include "jace/proxy/com/vividsolutions/jts/geom/Polygon.h"
using jace::proxy::com::vividsolutions::jts::geom::Polygon;
#include "jace/proxy/com/vividsolutions/jts/geom/Coordinate.h"
using jace::proxy::com::vividsolutions::jts::geom::Coordinate;
#include "jace/proxy/com/vividsolutions/jts/geom/GeometryFactory.h"
using jace::proxy::com::vividsolutions::jts::geom::GeometryFactory;

#include "jace/proxy/org/apache/accumulo/core/client/AccumuloException.h"
using jace::proxy::org::apache::accumulo::core::client::AccumuloException;
#include "jace/proxy/org/apache/accumulo/core/client/AccumuloSecurityException.h"
using jace::proxy::org::apache::accumulo::core::client::AccumuloSecurityException;

#include "jace/proxy/org/opengis/feature/simple/SimpleFeatureType.h"
using jace::proxy::org::opengis::feature::simple::SimpleFeatureType;
#include "jace/proxy/org/opengis/feature/type/GeometryDescriptor.h"
using jace::proxy::org::opengis::feature::type::GeometryDescriptor;

#include "jace/proxy/mil/nga/giat/geowave/accumulo/AccumuloDataStore.h"
using jace::proxy::mil::nga::giat::geowave::accumulo::AccumuloDataStore;
#include "jace/proxy/mil/nga/giat/geowave/accumulo/metadata/AccumuloDataStatisticsStore.h"
using jace::proxy::mil::nga::giat::geowave::accumulo::metadata::AccumuloDataStatisticsStore;
#include "jace/proxy/mil/nga/giat/geowave/accumulo/metadata/AccumuloAdapterStore.h"
using jace::proxy::mil::nga::giat::geowave::accumulo::metadata::AccumuloAdapterStore;

#include "jace/proxy/mil/nga/giat/geowave/index/ByteArrayId.h"
using jace::proxy::mil::nga::giat::geowave::index::ByteArrayId;
#include "jace/proxy/mil/nga/giat/geowave/store/CloseableIterator.h"
using jace::proxy::mil::nga::giat::geowave::store::CloseableIterator;
#include "jace/proxy/mil/nga/giat/geowave/store/adapter/DataAdapter.h"
using jace::proxy::mil::nga::giat::geowave::store::adapter::DataAdapter;
#include "jace/proxy/mil/nga/giat/geowave/store/adapter/statistics/StatisticalDataAdapter.h"
using jace::proxy::mil::nga::giat::geowave::store::adapter::statistics::StatisticalDataAdapter;
#include "jace/proxy/mil/nga/giat/geowave/store/index/Index.h"
using jace::proxy::mil::nga::giat::geowave::store::index::Index;
#include "jace/proxy/mil/nga/giat/geowave/store/index/IndexType_JaceIndexType.h"
using jace::proxy::mil::nga::giat::geowave::store::index::IndexType_JaceIndexType;
#include "jace/proxy/mil/nga/giat/geowave/store/query/Query.h"
using jace::proxy::mil::nga::giat::geowave::store::query::Query;
#include "jace/proxy/mil/nga/giat/geowave/store/query/SpatialQuery.h"
using jace::proxy::mil::nga::giat::geowave::store::query::SpatialQuery;

#include "jace/proxy/mil/nga/giat/geowave/store/adapter/statistics/DataStatistics.h"
using jace::proxy::mil::nga::giat::geowave::store::adapter::statistics::DataStatistics;
#include "jace/proxy/mil/nga/giat/geowave/store/adapter/statistics/BoundingBoxDataStatistics.h"
using jace::proxy::mil::nga::giat::geowave::store::adapter::statistics::BoundingBoxDataStatistics;

#include "jace/proxy/mil/nga/giat/geowave/vector/adapter/FeatureDataAdapter.h"
using jace::proxy::mil::nga::giat::geowave::vector::adapter::FeatureDataAdapter;
#include "jace/proxy/mil/nga/giat/geowave/vector/stats/FeatureBoundingBoxStatistics.h"
using jace::proxy::mil::nga::giat::geowave::vector::stats::FeatureBoundingBoxStatistics;

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

using mapnik::datasource;
using mapnik::parameters;

DATASOURCE_PLUGIN(geowave_datasource)

geowave_datasource::geowave_datasource(parameters const& params)
    :  datasource(params),
       desc_(geowave_datasource::name(), *params.get<std::string>("encoding","utf-8")),
       extent_(),
       zookeeper_url_(*params.get<std::string>("zookeeper_url", "")),
       instance_name_(*params.get<std::string>("instance_name", "")),
       username_(*params.get<std::string>("username", "")),
       password_(*params.get<std::string>("password", "")),
       table_namespace_(*params.get<std::string>("table_namespace", "")),
       adapter_id_(*params.get<std::string>("adapter_id", ""))
{
    this->init(params);
}

void geowave_datasource::init(mapnik::parameters const& params)
{
    // Initialize JVM
    if (!jace::isRunning()){
        int status = createJvm();
        if (status == 0)
            MAPNIK_LOG_DEBUG(geowave) << "JVM Creation Successful";
        else
            MAPNIK_LOG_DEBUG(geowave) << "JVM Creation Failed: Error ["  << status << "]";
    }

    // Initialize GeoWave interface
    try 
    {
        accumulo_operations_ = java_new<BasicAccumuloOperations>(
            java_new<String>(zookeeper_url_),
            java_new<String>(instance_name_),
            java_new<String>(username_),
            java_new<String>(password_),
            java_new<String>(table_namespace_));
    }
    catch (AccumuloException& e)
    {
        MAPNIK_LOG_DEBUG(geowave) << "There was a problem establishing a connector. " << e;
        return;
    }
    catch (AccumuloSecurityException& e)
    {
        MAPNIK_LOG_DEBUG(geowave) << "The credentials passed are invalid. " << e;
        return;
    }
        
    // pull bounds from statistics
    AccumuloDataStatisticsStore accumulo_statstore = java_new<AccumuloDataStatisticsStore>(
        accumulo_operations_);

    AccumuloAdapterStore accumulo_adapter_store = java_new<AccumuloAdapterStore>(
        accumulo_operations_);

    DataAdapter data_adapter = accumulo_adapter_store.getAdapter(java_new<ByteArrayId>(adapter_id_));
    
    if (!instanceof<FeatureDataAdapter>(data_adapter))
    {
        MAPNIK_LOG_DEBUG(geowave) << "Adapter type not supported for adapter_id: [" << adapter_id_ << "]";
        return;
    }
    
    FeatureDataAdapter feature_data_adapter = java_cast<FeatureDataAdapter>(data_adapter);

    FeatureBoundingBoxStatistics bbox_stats = java_cast<FeatureBoundingBoxStatistics>(accumulo_statstore.getDataStatistics(
        java_new<ByteArrayId>(adapter_id_), 
        FeatureBoundingBoxStatistics::composeId(feature_data_adapter.getType().getGeometryDescriptor().getLocalName()),
        JArray<String>(0)));
    
    if (!bbox_stats.isNull()){
        extent_.init(
            bbox_stats.getMinX(), 
            bbox_stats.getMinY(), 
            bbox_stats.getMaxX(), 
            bbox_stats.getMaxY());
    }
}

int geowave_datasource::createJvm()
{
    try
    {
        StaticVmLoader loader(JNI_VERSION_1_2);

        std::string jaceClasspath = TOSTRING(GEOWAVE_JACE_RUNTIME_JAR);
        std::string geowaveClasspath = TOSTRING(GEOWAVE_RUNTIME_JAR);

        OptionList options;
        //options.push_back(CustomOption("-Xdebug"));
        //options.push_back(CustomOption("-Xrunjdwp:server=y,transport=dt_socket,address=4000,suspend=y"));
        //options.push_back(CustomOption("-Xcheck:jni"));
        //options.push_back(Verbose (Verbose::JNI));
        //options.push_back(Verbose (Verbose::CLASS));
        options.push_back(ClassPath(jaceClasspath + os_pathsep + geowaveClasspath));

        jace::createVm(loader, options);
    }
    catch (VirtualMachineShutdownError&)
    {
        MAPNIK_LOG_DEBUG(geowave) << "The JVM was terminated in mid-execution. ";
        return -1;
    }
    catch (JNIException& jniException)
    {
        MAPNIK_LOG_DEBUG(geowave) << "An unexpected JNI error has occured: " << jniException.what();
        return -2;
    }
    catch (std::exception& e)
    {
        MAPNIK_LOG_DEBUG(geowave) << "An unexpected C++ error has occurred: " << e.what();
        return -3;
    }

    return 0;
}

geowave_datasource::~geowave_datasource() { }

const char * geowave_datasource::name()
{
    return "geowave";
}

mapnik::datasource::datasource_t geowave_datasource::type() const
{
    return datasource::Vector;
}

mapnik::box2d<double> geowave_datasource::envelope() const
{
    return extent_;
}

boost::optional<mapnik::datasource::geometry_t> geowave_datasource::get_geometry_type() const
{
    return mapnik::datasource::Collection;
}

mapnik::layer_descriptor geowave_datasource::get_descriptor() const
{
    return desc_;
}

mapnik::featureset_ptr geowave_datasource::features(mapnik::query const& q) const
{
    // if the query box intersects our world extent then query for features
    mapnik::box2d<double> const& box = q.get_bbox();
    if (extent_.intersects(box))
    {
        GeometryFactory factory = java_new<GeometryFactory>();

        JDouble lonMin = box.minx();
        JDouble lonMax = box.maxx();
        JDouble latMin = box.miny();
        JDouble latMax = box.maxy();

        JArray<Coordinate> coordArray(5);
        coordArray[0] = java_new<Coordinate>(lonMin, latMin);
        coordArray[1] = java_new<Coordinate>(lonMax, latMin);
        coordArray[2] = java_new<Coordinate>(lonMax, latMax);
        coordArray[3] = java_new<Coordinate>(lonMin, latMax);
        coordArray[4] = java_new<Coordinate>(lonMin, latMin);

        AccumuloDataStore accumulo_datastore = java_new<AccumuloDataStore>(
            accumulo_operations_);

        return std::make_shared<geowave_featureset>(accumulo_datastore.query(IndexType_JaceIndexType::createSpatialVectorIndex(), java_new<SpatialQuery>(factory.createPolygon(coordArray))));
    }

    // otherwise return an empty featureset pointer
    return mapnik::featureset_ptr();
}

mapnik::featureset_ptr geowave_datasource::features_at_point(mapnik::coord2d const& pt, double tol) const
{
    // if the query box intersects our world extent then query for features
    mapnik::box2d<double> box(pt, pt);
    box.pad(tol);
    if (extent_.intersects(box))
    {
        GeometryFactory factory = java_new<GeometryFactory>();

        JDouble lonMin = box.minx();
        JDouble lonMax = box.maxx();
        JDouble latMin = box.miny();
        JDouble latMax = box.maxy();

        JArray<Coordinate> coordArray(5);
        coordArray[0] = java_new<Coordinate>(lonMin, latMin);
        coordArray[1] = java_new<Coordinate>(lonMax, latMin);
        coordArray[2] = java_new<Coordinate>(lonMax, latMax);
        coordArray[3] = java_new<Coordinate>(lonMin, latMax);
        coordArray[4] = java_new<Coordinate>(lonMin, latMin);

        AccumuloDataStore accumulo_datastore = java_new<AccumuloDataStore>(
            accumulo_operations_);
        
        return std::make_shared<geowave_featureset>(accumulo_datastore.query(IndexType_JaceIndexType::createSpatialVectorIndex(), java_new<SpatialQuery>(factory.createPolygon(coordArray))));
    }
    
    // otherwise return an empty featureset pointer
    return mapnik::featureset_ptr();
}
