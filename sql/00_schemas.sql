-- Создание схем для стейджинга и Data Vault
-- Схема stage для сырых данных, схема dv для Data Vault структур
-- Параллельно создаются алиасы с новыми именами: staging и datavault (для читабельности)

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS dv;

-- Новые схемы (мягкая миграция имен)
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS datavault;

COMMENT ON SCHEMA stage IS 'Staging area for raw loads from SampleSuperstore.csv';
COMMENT ON SCHEMA dv IS 'Data Vault (hubs, links, satellites)';
COMMENT ON SCHEMA staging IS 'Alias schema for stage (new name)';
COMMENT ON SCHEMA datavault IS 'Alias schema for dv (new name)';