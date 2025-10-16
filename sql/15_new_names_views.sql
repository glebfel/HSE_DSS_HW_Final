-- Мягкая миграция имен: создаем представления с новыми названиями,
-- которые ссылаются на существующие таблицы в схемах stage и dv.

-- Схемы создаются в 00_schemas.sql, здесь только представления.

-- New readable schema alias for stage
DROP VIEW IF EXISTS staging.sample_superstore;
CREATE VIEW staging.sample_superstore AS SELECT * FROM stage.sample_superstore;
COMMENT ON VIEW staging.sample_superstore IS 'Alias view for stage.sample_superstore';

-- New readable schema alias for dv -> datavault
-- Hubs -> hub_*
DROP VIEW IF EXISTS datavault.hub_ship_mode;
CREATE VIEW datavault.hub_ship_mode AS SELECT * FROM dv.h_ship_mode;

DROP VIEW IF EXISTS datavault.hub_segment;
CREATE VIEW datavault.hub_segment AS SELECT * FROM dv.h_segment;

DROP VIEW IF EXISTS datavault.hub_geography;
CREATE VIEW datavault.hub_geography AS SELECT * FROM dv.h_geography;

DROP VIEW IF EXISTS datavault.hub_category;
CREATE VIEW datavault.hub_category AS SELECT * FROM dv.h_category;

DROP VIEW IF EXISTS datavault.hub_sub_category;
CREATE VIEW datavault.hub_sub_category AS SELECT * FROM dv.h_sub_category;

-- Satellites -> sat_*
DROP VIEW IF EXISTS datavault.sat_ship_mode;
CREATE VIEW datavault.sat_ship_mode AS SELECT * FROM dv.s_ship_mode;

DROP VIEW IF EXISTS datavault.sat_segment;
CREATE VIEW datavault.sat_segment AS SELECT * FROM dv.s_segment;

DROP VIEW IF EXISTS datavault.sat_geography;
CREATE VIEW datavault.sat_geography AS SELECT * FROM dv.s_geography;

DROP VIEW IF EXISTS datavault.sat_category;
CREATE VIEW datavault.sat_category AS SELECT * FROM dv.s_category;

DROP VIEW IF EXISTS datavault.sat_sub_category;
CREATE VIEW datavault.sat_sub_category AS SELECT * FROM dv.s_sub_category;

DROP VIEW IF EXISTS datavault.sat_sale_metrics;
CREATE VIEW datavault.sat_sale_metrics AS SELECT * FROM dv.s_sale_metrics;

-- Links -> link_*
DROP VIEW IF EXISTS datavault.link_sale;
CREATE VIEW datavault.link_sale AS SELECT * FROM dv.l_sale;
