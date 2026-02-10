-- ref_admin.regions definition
CREATE TABLE ref_admin.regions (
	gid serial4 NOT NULL,
	nom_region varchar(100) NOT NULL,
	code_insee_region varchar(3) NOT NULL,
	geom geography(multipolygon, 4326) NULL, 
	CONSTRAINT regions_code_insee_region_key UNIQUE (code_insee_region),
	CONSTRAINT regions_pkey PRIMARY KEY (gid)
);
CREATE INDEX idx_regions_geom ON ref_admin.regions USING gist (geom);
CREATE INDEX idx_regions_nom_trgm ON ref_admin.regions USING gin (nom_region gin_trgm_ops);

COMMENT ON TABLE ref_admin.regions IS 'table des régions de France';
COMMENT ON COLUMN ref_admin.regions.nom_region IS 'toponyme de la région';

-- ref_admin.departements definition
CREATE TABLE ref_admin.departements (
 	gid serial4 NOT NULL,
 	code_insee_departement varchar(3) NOT NULL,
 	nom_departement varchar(100) NOT NULL,
 	code_insee_region varchar(3) NULL,
 	geom geography(multipolygon, 4326) NULL,
 	CONSTRAINT departements_code_insee_departement_key UNIQUE (code_insee_departement),
 	CONSTRAINT departements_pkey PRIMARY KEY (gid)
 );
CREATE INDEX idx_departements_geom ON ref_admin.departements USING gist (geom);
CREATE INDEX idx_departements_nom_trgm ON ref_admin.departements USING gin (nom_departement gin_trgm_ops);
ALTER TABLE ref_admin.departements ADD CONSTRAINT departements_code_insee_region_fkey FOREIGN KEY (code_insee_region) REFERENCES ref_admin.regions(code_insee_region);

COMMENT ON TABLE ref_admin.departements IS 'table des départements de France';
COMMENT ON COLUMN ref_admin.departements.nom_departement IS 'toponyme du département';

-- ref_admin.epci definition
CREATE TABLE ref_admin.epci (
	gid serial4 NOT NULL,
	code_insee_epci varchar(10) NOT NULL,
	nom_epci varchar(100) NOT NULL,
	geom geography(multipolygon, 4326) NULL, 
	CONSTRAINT epci_code_insee_epci_key UNIQUE (code_insee_epci),
	CONSTRAINT epci_pkey PRIMARY KEY (gid)
);
CREATE INDEX idx_epci_geom ON ref_admin.epci USING gist (geom);
CREATE INDEX idx_epci_nom_trgm ON ref_admin.epci USING gin (nom_epci gin_trgm_ops);

COMMENT ON TABLE ref_admin.epci IS 'table des EPCI (CA: Communautés d''agglomérations, CU: Communautés urbaines, CT: Communautés de communes) de France';
COMMENT ON COLUMN ref_admin.epci.nom_epci IS 'toponyme de l''EPCI';

-- ref_admin.communes definition
CREATE TABLE ref_admin.communes (
 	gid serial4 NOT NULL,
 	code_insee_commune varchar(5) NOT NULL,
 	nom_commune varchar(100) NOT NULL,
 	population int4 NULL,
	code_insee_epci varchar(10) NULL,
 	code_insee_departement varchar(3) NULL,
 	code_insee_region varchar(3) NULL,
 	geom geography(multipolygon, 4326) NULL,
 	CONSTRAINT communes_code_insee_commune_key UNIQUE (code_insee_commune),
 	CONSTRAINT communes_pkey PRIMARY KEY (gid)
);
CREATE INDEX idx_communes_geom ON ref_admin.communes USING spgist (geom);
CREATE INDEX idx_communes_nom_trgm ON ref_admin.communes USING gin (nom_commune gin_trgm_ops);
ALTER TABLE ref_admin.communes ADD CONSTRAINT communes_code_insee_epci_fkey FOREIGN KEY (code_insee_epci) REFERENCES ref_admin.epci(code_insee_epci);
ALTER TABLE ref_admin.communes ADD CONSTRAINT communes_code_insee_departement_fkey FOREIGN KEY (code_insee_departement) REFERENCES ref_admin.departements(code_insee_departement);
ALTER TABLE ref_admin.communes ADD CONSTRAINT communes_code_insee_region_fkey FOREIGN KEY (code_insee_region) REFERENCES ref_admin.regions(code_insee_region);

COMMENT ON TABLE ref_admin.communes IS 'table des communes de France';
COMMENT ON COLUMN ref_admin.communes.nom_commune IS 'toponyme de la commune';