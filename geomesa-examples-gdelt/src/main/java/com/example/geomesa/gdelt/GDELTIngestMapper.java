package com.example.geomesa.gdelt;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class GDELTIngestMapper extends Mapper<LongWritable, Text, Text, SimpleFeature> {

    private static int LATITUDE_COL_IDX  = 39;
    private static int LONGITUDE_COL_IDX = 40;
    private static int DATE_COL_IDX = 1;
    private static int ID_COL_IDX = 0;
    private static int MINIMUM_NUM_FIELDS = 41;

    private SimpleFeatureBuilder featureBuilder;
    private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    @Override
    public void setup(Mapper<LongWritable, Text, Text, SimpleFeature>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        String featureName = context.getConfiguration().get(GDELTIngest.FEATURE_NAME);
        try {
            SimpleFeatureType featureType = GDELTIngest.buildGDELTFeatureType(featureName);
            featureBuilder = new SimpleFeatureBuilder(featureType);
        } catch (Exception e) {
            throw new IOException("Error setting up feature type", e);
        }
    }

    @Override
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, SimpleFeature>.Context context)
            throws IOException, InterruptedException {
        String[] attributes = value.toString().split("\\t", -1);
        if (attributes.length >= MINIMUM_NUM_FIELDS && !attributes[LATITUDE_COL_IDX].equals("") &&
            !attributes[LONGITUDE_COL_IDX].equals("")) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
            try {
                featureBuilder.reset();

                Double lat = Double.parseDouble(attributes[LATITUDE_COL_IDX]);
                Double lon = Double.parseDouble(attributes[LONGITUDE_COL_IDX]);
                if (Math.abs(lat) > 90.0 || Math.abs(lon) > 180.0) {
                    context.getCounter("com.example.geomesa", "invalid-geoms").increment(1);
                } else {
                    Geometry geom = geometryFactory.createPoint(new Coordinate(lon, lat));
                    SimpleFeature simpleFeature = featureBuilder.buildFeature(attributes[ID_COL_IDX]);
                    int i = 0;
                    while (i < attributes.length) {
                        simpleFeature.setAttribute(i, attributes[i]);
                        i++;
                    }
                    simpleFeature.setAttribute("SQLDATE", formatter.parse(attributes[DATE_COL_IDX]));
                    simpleFeature.setDefaultGeometry(geom);

                    context.write(new Text(), simpleFeature);
                }
            } catch (ParseException e) {
                context.getCounter("com.example.geomesa", "parse-errors").increment(1);
                e.printStackTrace();
            }
        } else {
            context.getCounter("com.example.geomesa", "invalid-lines").increment(1);
        }
    }
}
