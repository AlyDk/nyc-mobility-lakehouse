-- Fleet master data. Runs on SQL Server and Azure SQL (target database chosen with sqlcmd -d).
-- Vendor ids and names follow the TLC data dictionary; platform_fee_pct is synthetic (SCD2 demo).
IF SCHEMA_ID('ref') IS NULL EXEC('CREATE SCHEMA ref');
GO
IF OBJECT_ID('ref.vendor') IS NULL
CREATE TABLE ref.vendor (
    vendor_id        INT           NOT NULL PRIMARY KEY,
    vendor_name      NVARCHAR(100) NOT NULL,
    platform_fee_pct DECIMAL(5,2)  NOT NULL,
    is_active        BIT           NOT NULL DEFAULT 1,
    updated_at       DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
MERGE ref.vendor AS t
USING (VALUES (1, N'Creative Mobile Technologies', 3.00),
              (2, N'Curb Mobility', 3.50),
              (6, N'Myle Technologies', 4.00),
              (7, N'Helix', 4.00)) AS s (vendor_id, vendor_name, platform_fee_pct)
ON t.vendor_id = s.vendor_id
WHEN NOT MATCHED THEN
    INSERT (vendor_id, vendor_name, platform_fee_pct) VALUES (s.vendor_id, s.vendor_name, s.platform_fee_pct);
GO