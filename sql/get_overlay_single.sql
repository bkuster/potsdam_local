WITH volume AS(
SELECT cityobject_id AS vol_id,
    realval AS volume
    FROM cityobject_genericattrib
    WHERE attrname = 'volume'
)
SELECT (i_co2/volume)*1000,
  ST_AsText(ST_Transform(geom, 4326))
  FROM warmekataster.bestand
INNER JOIN building
  ON building.name = a_objektna
INNER JOIN volume
  ON vol_id = building.id
WHERE ST_Intersects(
  ST_Transform(geom, 4326),
  ST_GeomFromText('%s', 4326)
  )
AND ST_NumGeometries(geom) = 1
;
