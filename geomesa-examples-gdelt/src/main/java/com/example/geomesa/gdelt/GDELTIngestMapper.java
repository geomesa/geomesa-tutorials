package com.example.geomesa.gdelt;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class GDELTIngestMapper extends Mapper<LongWritable,Text,Key,Value> {

    private static int LATITUDE_COL_IDX  = 39;
    private static int LONGITUDE_COL_IDX = 40;
    private static int DATE_COL_IDX = 1;
    private static int ID_COL_IDX = 0;
    private static int MINIMUM_NUM_FIELDS = 41;

    private SimpleFeatureType featureType = null;
    private FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter = null;
    private SimpleFeatureBuilder featureBuilder;
    private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    public void setup(Mapper<LongWritable,Text,Key,Value>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Map<String , String> connectionParams = new HashMap<String , String>();
        connectionParams.put("instanceId", context.getConfiguration().get("instanceId"));
        connectionParams.put("zookeepers", context.getConfiguration().get("zookeepers"));
        connectionParams.put("user", context.getConfiguration().get("user"));
        connectionParams.put("password", context.getConfiguration().get("password"));
        connectionParams.put("auths", context.getConfiguration().get("auths"));
        connectionParams.put("visibilities", context.getConfiguration().get("visibilities"));
        connectionParams.put("tableName", context.getConfiguration().get("tableName"));

        String featureName = context.getConfiguration().get("featureName");

        DataStore ds = DataStoreFinder.getDataStore(connectionParams);
        featureType = ds.getSchema(featureName);
        featureBuilder = new SimpleFeatureBuilder(featureType);
        featureWriter = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT);
    }

    public void map(LongWritable key, Text value, Mapper<LongWritable,Text,Key,Value>.Context context) {
        String[] attributes = value.toString().split("\\t", -1);
        if (attributes.length >= MINIMUM_NUM_FIELDS && !attributes[LATITUDE_COL_IDX].equals("") && !attributes[LONGITUDE_COL_IDX].equals("")) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            try {
                featureBuilder.reset();
                featureBuilder.addAll(attributes);

                Double lat = Double.parseDouble(attributes[LATITUDE_COL_IDX]);
                Double lon = Double.parseDouble(attributes[LONGITUDE_COL_IDX]);
                Geometry geom = geometryFactory.createPoint(new Coordinate(lon, lat));
                SimpleFeature simpleFeature = featureBuilder.buildFeature(attributes[ID_COL_IDX]);
                simpleFeature.setAttribute("SQLDATE", formatter.parse(attributes[DATE_COL_IDX]));
                simpleFeature.setDefaultGeometry(geom);

                try {
                    SimpleFeature next = featureWriter.next();
                    for (int i = 0; i < simpleFeature.getAttributeCount(); i++) {
                        next.setAttribute(i, simpleFeature.getAttribute(i));
                    }
                    ((FeatureIdImpl)next.getIdentifier()).setID(simpleFeature.getID());
                    featureWriter.write();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
