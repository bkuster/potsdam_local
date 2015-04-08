WITH poly AS(
  SELECT ST_AsText(ST_Extent(ST_Transform(geom, 4326))) AS poly
  FROM warmekataster.bestand
)
SELECT ST_AsText(cell) FROM
  (SELECT
    (ST_Dump(makegrid_2d(
      ST_GeomFromText(poly, 4326), 5000))).geom AS cell FROM poly) AS q_grid
;
