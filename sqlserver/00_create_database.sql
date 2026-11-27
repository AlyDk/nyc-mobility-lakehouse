-- On-prem SQL Server only. Azure SQL databases are created in the Azure portal.
IF DB_ID('fleet_master') IS NULL CREATE DATABASE fleet_master;
GO