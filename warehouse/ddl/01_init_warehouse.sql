-- =============================================================================
-- Senbon — Retail Intelligence & Analytics Engine - Warehouse Initialization
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Schema Namespaces for Data Platform Architecture
CREATE SCHEMA IF NOT EXISTS staging;       -- Raw staged lakehouse ingestion
CREATE SCHEMA IF NOT EXISTS intermediate;  -- Enriched pre-aggregation models
CREATE SCHEMA IF NOT EXISTS analytics;     -- Star Schema Marts (Facts & Dimensions)
CREATE SCHEMA IF NOT EXISTS monitoring;    -- Pipeline SLAs, DQ metrics, and Audit logs

COMMENT ON SCHEMA staging IS 'Staging layer populated from lakehouse Silver zone';
COMMENT ON SCHEMA intermediate IS 'Normalized intermediate business entities';
COMMENT ON SCHEMA analytics IS 'Production dimensional star schema for BI and dashboards';
COMMENT ON SCHEMA monitoring IS 'Data platform observability, pipeline SLAs, and audit trails';

