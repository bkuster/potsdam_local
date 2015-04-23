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
  SELECT (sum(realval)/sum(volume))*1000 AS co2, sector_name
    FROM co2
    GROUP BY sector_name
    ORDER BY co2 ASC
), var AS(
SELECT ((i_co2/volume)*1000) - co2 AS norm,
  ntile(5) OVER (ORDER BY ((i_co2/volume)*1000) - co2) AS cume
  FROM building
  LEFT JOIN mapping.building_sector_{0}
      ON building_sector_{0}.id = building.id
  LEFT JOIN summed
      ON summed.sector_name = mapping.building_sector_{0}.sector_name
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
