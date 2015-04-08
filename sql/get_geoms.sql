WITH
            thematic_surf AS (
        SELECT building.id AS b_id, lod2_multi_surface_id
        FROM thematic_surface INNER JOIN building
        ON thematic_surface.building_id=building.id
        WHERE building.id = %s
            )
            SELECT ST_AsText(ST_Transform(geometry, 4326))
            FROM  thematic_surf INNER JOIN surface_geometry
            ON thematic_surf.lod2_multi_surface_id=surface_geometry.root_id
            WHERE geometry IS NOT NULL;
