WITH thematic_surf AS (
  SELECT building.id AS b_id, lod2_multi_surface_id
  FROM thematic_surface INNER JOIN building
  ON thematic_surface.building_id=building.id
  WHERE building.id IN (
	  SELECT cityobject_id AS vol_id
  	FROM cityobject_genericattrib
	  WHERE attrname = 'i_co2')
), geoms AS(
SELECT b_id, ST_Transform(geometry, 4326) AS geom
  FROM  thematic_surf
  INNER JOIN surface_geometry
  ON thematic_surf.lod2_multi_surface_id=surface_geometry.root_id
  WHERE geometry IS NOT NULL
)
SELECT DISTINCT b_id FROM fishnet INNER JOIN geoms ON ST_Intersects(geoms.geom, fishnet.geom)
WHERE fishnet.id = %s
ORDER BY b_id ASC;
