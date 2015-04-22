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
SELECT cityobject_id,
    (co2/vol_sum) * 1000,
    street || ' ' ||house_number AS address,
    sector,
    building.description,
  warmekataster.rt_gebaude_typ.description,
  warmekataster.rt_baujahr.description,
  warmekataster.rt_sanierung.description
    FROM cityobject_genericattrib
    LEFT JOIN mapping.building_sector_{0}
      ON building_sector_{0}.id = cityobject_genericattrib.cityobject_id
    LEFT JOIN summed
      ON sector_name = sector
    INNER JOIN address_to_building
      ON building_id = cityobject_id
    INNER JOIN address
      ON address.id = address_to_building.address_id
    INNER JOIN building ON
      cityobject_id = building.id
    INNER JOIN warmekataster.bestand
      ON building.name = a_objektna
    LEFT JOIN warmekataster.rt_gebaude_typ
      ON b_gebaeude = rt_gebaude_typ.type_id
    LEFT JOIN warmekataster.rt_baujahr
      ON CAST(b_baujahr_ AS integer) = rt_baujahr.baujahr_id
    LEFT JOIN warmekataster.rt_sanierung
      ON b_sanierun = rt_sanierung.sanierung_id
    WHERE attrname = 'i_co2' AND cityobject_id = {1:d};
