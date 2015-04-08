WITH volume AS(
SELECT cityobject_id AS vol_id,
    realval AS volume
    FROM cityobject_genericattrib
    WHERE attrname = 'volume'
), var AS(
SELECT (i_co2/volume)*1000 AS norm,
  ntile(5) OVER (ORDER BY (i_co2/volume)*1000) AS cume
  FROM building
  INNER JOIN warmekataster.bestand
    ON building.name = a_objektna
  INNER JOIN volume
    ON vol_id = building.id
  WHERE i_co2 > 0.00001
)
SELECT max(norm) AS break
FROM var
GROUP BY cume
ORDER BY cume
;
