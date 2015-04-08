WITH volume AS(
SELECT cityobject_id AS vol_id,
    realval AS volume
    FROM cityobject_genericattrib
    WHERE attrname = 'volume'
)
SELECT MIN((i_co2/volume)*1000), MAX((i_co2/volume)*1000)
  FROM building
  INNER JOIN warmekataster.bestand
    ON building.name = a_objektna
  INNER JOIN volume
    ON vol_id = building.id
  WHERE i_co2 > 0.00001
;
