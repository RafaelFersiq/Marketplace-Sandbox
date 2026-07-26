-- ============================================================================
-- Infrastructure Initialization Script for PostgreSQL
-- Target Container Path: /docker-entrypoint-initdb.d/init.sql
-- Database context on initial entry: 'marketplace' (default database)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Apache Airflow Metadata Database Setup
-- ----------------------------------------------------------------------------
-- Create dedicated database 'airflow' for storing Apache Airflow orchestration metadata.
CREATE DATABASE airflow;

-- Grant all administrative privileges on database 'airflow' to superuser 'postgres'.
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

-- ----------------------------------------------------------------------------
-- 2. Medallion & Transactional Schemas Setup in Primary Database ('marketplace')
-- ----------------------------------------------------------------------------
-- Create schema 'transactional' for raw relational marketplace transaction simulation.
CREATE SCHEMA IF NOT EXISTS transactional;

-- Create schema 'staging' for initial dbt data cleaning, casting, and renaming layer.
CREATE SCHEMA IF NOT EXISTS staging;

-- Create schema 'intermediate' for dbt pre-aggregations and business logic joins.
CREATE SCHEMA IF NOT EXISTS intermediate;

-- Create schema 'marts' for Gold layer dimensional star-schema analytical tables.
CREATE SCHEMA IF NOT EXISTS marts;
