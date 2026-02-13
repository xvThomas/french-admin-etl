-- demography.population_commune definition
CREATE TABLE demography.population_commune (
	code_insee_commune varchar(5) NOT NULL,
	annee smallint NOT NULL CHECK (annee >= 1900 AND annee <= 2100),
	pop int4 NULL CHECK (pop >= 0),
	pop_h int4 NULL CHECK (pop_h >= 0),
	pop_f int4 NULL CHECK (pop_f >= 0),
	pop_LT15 int4 NULL CHECK (pop_LT15 >= 0),
	pop_LT15_h int4 NULL CHECK (pop_LT15_h >= 0),
	pop_LT15_f int4 NULL CHECK (pop_LT15_f >= 0),
	pop_LT20 int4 NULL CHECK (pop_LT20 >= 0),
	pop_LT20_h int4 NULL CHECK (pop_LT20_h >= 0),
	pop_LT20_f int4 NULL CHECK (pop_LT20_f >= 0),
	pop_15T24 int4 NULL CHECK (pop_15T24 >= 0),
	pop_15T24_h int4 NULL CHECK (pop_15T24_h >= 0),
	pop_15T24_f int4 NULL CHECK (pop_15T24_f >= 0),
	pop_20T64 int4 NULL CHECK (pop_20T64 >= 0),
	pop_20T64_h int4 NULL CHECK (pop_20T64_h >= 0),
	pop_20T64_f int4 NULL CHECK (pop_20T64_f >= 0),
	pop_25T39 int4 NULL CHECK (pop_25T39 >= 0),
	pop_25T39_h int4 NULL CHECK (pop_25T39_h >= 0),
	pop_25T39_f int4 NULL CHECK (pop_25T39_f >= 0),
	pop_40T54 int4 NULL CHECK (pop_40T54 >= 0),
	pop_40T54_h int4 NULL CHECK (pop_40T54_h >= 0),
	pop_40T54_f int4 NULL CHECK (pop_40T54_f >= 0),
	pop_55T64 int4 NULL CHECK (pop_55T64 >= 0),
	pop_55T64_h int4 NULL CHECK (pop_55T64_h >= 0),
	pop_55T64_f int4 NULL CHECK (pop_55T64_f >= 0),
	pop_65T79 int4 NULL CHECK (pop_65T79 >= 0),
	pop_65T79_h int4 NULL CHECK (pop_65T79_h >= 0),
	pop_65T79_f int4 NULL CHECK (pop_65T79_f >= 0),
	pop_GE65 int4 NULL CHECK (pop_GE65 >= 0),
	pop_GE65_h int4 NULL CHECK (pop_GE65_h >= 0),
	pop_GE65_f int4 NULL CHECK (pop_GE65_f >= 0),
	pop_GE80 int4 NULL CHECK (pop_GE80 >= 0),
	pop_GE80_h int4 NULL CHECK (pop_GE80_h >= 0),
	pop_GE80_f int4 NULL CHECK (pop_GE80_f >= 0),

	CONSTRAINT population_commune_pkey PRIMARY KEY (code_insee_commune, annee)
);

CREATE INDEX idx_population_commune_annee ON demography.population_commune (annee);
ALTER TABLE demography.population_commune ADD CONSTRAINT population_commune_code_insee_commune_fkey FOREIGN KEY (code_insee_commune) REFERENCES ref_admin.communes(code_insee_commune);

COMMENT ON TABLE demography.population_commune IS 'table de la population française par commune et par année';
COMMENT ON COLUMN demography.population_commune.code_insee_commune IS 'code INSEE de la commune';
COMMENT ON COLUMN demography.population_commune.annee IS 'année de référence de la population';
COMMENT ON COLUMN demography.population_commune.pop IS 'population totale de la commune';
COMMENT ON COLUMN demography.population_commune.pop_h IS 'population masculine de la commune';
COMMENT ON COLUMN demography.population_commune.pop_f IS 'population féminine de la commune';
COMMENT ON COLUMN demography.population_commune.pop_LT15 IS 'population de moins de 15 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_LT15_h IS 'population masculine de moins de 15 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_LT15_f IS 'population féminine de moins de 15 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_LT20 IS 'population de moins de 20 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_LT20_h IS 'population masculine de moins de 20 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_LT20_f IS 'population féminine de moins de 20 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_15T24 IS 'population de 15 à 24 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_15T24_h IS 'population masculine de 15 à 24 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_15T24_f IS 'population féminine de 15 à 24 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_20T64 IS 'population de 20 à 64 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_20T64_h IS 'population masculine de 20 à 64 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_20T64_f IS 'population féminine de 20 à 64 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_25T39 IS 'population de 25 à 39 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_25T39_h IS 'population masculine de 25 à 39 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_25T39_f IS 'population féminine de 25 à 39 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_40T54 IS 'population de 40 à 54 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_40T54_h IS 'population masculine de 40 à 54 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_40T54_f IS 'population féminine de 40 à 54 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_55T64 IS 'population de 55 à 64 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_55T64_h IS 'population masculine de 55 à 64 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_55T64_f IS 'population féminine de 55 à 64 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_65T79 IS 'population de 65 à 79 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_65T79_h IS 'population masculine de 65 à 79 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_65T79_f IS 'population féminine de 65 à 79 ans de la commune';
COMMENT ON COLUMN demography.population_commune.pop_GE65 IS 'population de 65 ans et plus de la commune';
COMMENT ON COLUMN demography.population_commune.pop_GE65_h IS 'population masculine de 65 ans et plus de la commune';
COMMENT ON COLUMN demography.population_commune.pop_GE65_f IS 'population féminine de 65 ans et plus de la commune';
COMMENT ON COLUMN demography.population_commune.pop_GE80 IS 'population de 80 ans et plus de la commune';
COMMENT ON COLUMN demography.population_commune.pop_GE80_h IS 'population masculine de 80 ans et plus de la commune';
COMMENT ON COLUMN demography.population_commune.pop_GE80_f IS 'population féminine de 80 ans et plus de la commune';
