-- Databricks notebook source
-- Phase 1: Unity Catalog layout for nyc-mobility-lakehouse

CREATE CATALOG IF NOT EXISTS nyc_mobility;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.bronze;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.silver;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.gold;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.ops;
CREATE VOLUME IF NOT EXISTS nyc_mobility.bronze.landing;
SHOW SCHEMAS IN nyc_mobility;