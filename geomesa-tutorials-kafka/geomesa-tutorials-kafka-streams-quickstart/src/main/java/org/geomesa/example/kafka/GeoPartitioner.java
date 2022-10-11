package org.geomesa.example.kafka;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.locationtech.geomesa.curve.Z2SFC;
import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;
import org.locationtech.geomesa.utils.geohash.GeohashUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.List;

class GeoPartitioner implements KeyValueMapper<String, GeoMesaMessage, String> {

  private final Short partitionNumBits;
  private final Integer defaultGeomIndex;
  private final Z2SFC z2;

  public GeoPartitioner(Short partitionNumBits, Integer defaultGeomIndex) {
    this.partitionNumBits = partitionNumBits;
    this.defaultGeomIndex = defaultGeomIndex;
    this.z2 = new Z2SFC(partitionNumBits / 2);
  }

  private String getZBin(Geometry geom) {
    Point safeGeom = GeohashUtils.getInternationalDateLineSafeGeometry(geom).get().getCentroid();
    Long index = z2.index(safeGeom.getX(), safeGeom.getY(), false);
    return String.format("%0" + partitionNumBits + "d", index);
  }

  @Override
  public String apply(String key, GeoMesaMessage value) {
    List<Object> attributes = value.asJava();
    Geometry geom = (Geometry) attributes.get(defaultGeomIndex);
    return getZBin(geom);
  }
}