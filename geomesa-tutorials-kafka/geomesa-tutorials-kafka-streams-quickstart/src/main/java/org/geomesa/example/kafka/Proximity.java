package org.geomesa.example.kafka;

import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class Proximity {
  GeoMesaMessage message1;
  GeoMesaMessage message2;
  Integer defaultGeomIndex;
  GeodeticCalculator gc;

  public Proximity(GeoMesaMessage message1, GeoMesaMessage message2, Integer defaultGeomIndex) {
    this.message1 = message1;
    this.message2 = message2;
    this.defaultGeomIndex = defaultGeomIndex;
    this.gc = new GeodeticCalculator();
  }

  public Double getDistance() {
    Geometry geom1 = (Geometry) message1.attributes().apply(defaultGeomIndex);
    Geometry geom2 = (Geometry) message2.attributes().apply(defaultGeomIndex);
    gc.setStartingGeographicPoint(geom1.getCoordinate().x, geom1.getCoordinate().y);
    gc.setDestinationGeographicPoint(geom2.getCoordinate().x, geom2.getCoordinate().y);
    return gc.getOrthodromicDistance();
  }

  private String getFID(GeoMesaMessage message) {
    return message.attributes().apply(0).toString();
  }

  public GeoMesaMessage toGeoMesaMessage() {
    List<Object> attributes = new ArrayList<>();
    attributes.add("proximity-" + getFID(message1) + "-" + getFID(message2));
    attributes.add(message1.attributes().apply(1));
    attributes.add(message1.attributes().apply(defaultGeomIndex));
    return GeoMesaMessage.upsert(attributes);
  }

  public Boolean areDifferent() {
    return !Objects.equals(getFID(message1), getFID(message2));
  }

  public Boolean areNotProximities() {
    return !getFID(message1).startsWith("proximity") && !getFID(message2).startsWith("proximity");
  }
}