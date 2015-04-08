SELECT ST_AsText(geom)
FROM fishnet
WHERE id = %s
;