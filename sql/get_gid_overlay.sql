SELECT gid FROM warmekataster.bestand
WHERE ST_Intersects(geom, ST_GeomFromText(%s, 4326));