WITH volume AS(
SELECT cityobject_id AS vol_id,
    realval AS volume
    FROM cityobject_genericattrib
    WHERE attrname = 'volume'
), co2 AS (
    SELECT *
      FROM mapping.building_sector_{0}
    INNER JOIN public.cityobject_genericattrib
      ON building_sector_{0}.id=cityobject_genericattrib.cityobject_id
    INNER JOIN volume
      ON cityobject_id = vol_id
    WHERE attrname='i_co2'
), summed AS(
    SELECT sum(realval) AS co2,
      sum(volume) AS vol_sum,
      sector_name AS sector
    FROM co2
    GROUP BY sector_name
)
SELECT (co2/vol_sum) * 1000,
  ST_AsText(ST_Transform(geom, 4326))
  FROM warmekataster.bestand

INNER JOIN building
  ON building.name = a_objektna
INNER JOIN mapping.building_sector_{0}
  ON building_sector_{0}.id = building.id
INNER JOIN summed
  ON sector_name = sector

WHERE ST_Intersects(
  ST_Transform(geom, 4326),
  ST_GeomFromText('{1}', 4326)
  )
AND ST_NumGeometries(geom) = 1
;
