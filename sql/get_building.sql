WITH volume AS(
SELECT cityobject_id AS vol_id,
    realval AS volume
    FROM cityobject_genericattrib
    WHERE attrname = 'volume'
)
SELECT cityobject_id,
    i_co2/volume * 1000,
    street || ' ' ||house_number AS address,
    alkis.building_function.description,
    building.description,
  warmekataster.rt_gebaude_typ.description,
  warmekataster.rt_baujahr.description,
  warmekataster.rt_sanierung.description
    FROM cityobject_genericattrib
    INNER JOIN address_to_building
      ON building_id = cityobject_id
    INNER JOIN address
      ON address.id = address_to_building.address_id
    INNER JOIN building ON
      cityobject_id = building.id
    INNER JOIN warmekataster.bestand
      ON building.name = a_objektna
    INNER JOIN alkis.building_function
      ON CAST(function AS integer)= building_function_id
    INNER JOIN volume ON
      vol_id = cityobject_id
    LEFT JOIN warmekataster.rt_gebaude_typ
      ON b_gebaeude = rt_gebaude_typ.type_id
    LEFT JOIN warmekataster.rt_baujahr
      ON CAST(b_baujahr_ AS integer) = rt_baujahr.baujahr_id
    LEFT JOIN warmekataster.rt_sanierung
      ON b_sanierun = rt_sanierung.sanierung_id
    WHERE attrname = 'i_co2' AND cityobject_id = %s;
