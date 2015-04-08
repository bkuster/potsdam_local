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
)
SELECT (sum(realval)/sum(volume))*1000 AS co2, sector_name
FROM co2
GROUP BY sector_name
ORDER BY co2 ASC
;
