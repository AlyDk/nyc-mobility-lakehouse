-- Reference data, change procedure with audit log, and extract views (SQL Server, T-SQL).

-- TLC rate codes (from the TLC data dictionary)
IF OBJECT_ID('ref.rate_code') IS NULL
CREATE TABLE ref.rate_code (
    rate_code_id   INT          NOT NULL PRIMARY KEY,
    rate_code_desc NVARCHAR(50) NOT NULL
);
GO
MERGE ref.rate_code AS t
USING (VALUES (1, N'Standard rate'), (2, N'JFK'), (3, N'Newark'), (4, N'Nassau or Westchester'),
              (5, N'Negotiated fare'), (6, N'Group ride'), (99, N'Null/unknown')) AS s (rate_code_id, rate_code_desc)
ON t.rate_code_id = s.rate_code_id
WHEN MATCHED AND t.rate_code_desc <> s.rate_code_desc THEN UPDATE SET rate_code_desc = s.rate_code_desc
WHEN NOT MATCHED THEN INSERT (rate_code_id, rate_code_desc) VALUES (s.rate_code_id, s.rate_code_desc);
GO

-- Audit trail for vendor fee changes
IF OBJECT_ID('ref.vendor_change_log') IS NULL
CREATE TABLE ref.vendor_change_log (
    change_id   INT IDENTITY(1, 1) PRIMARY KEY,
    vendor_id   INT          NOT NULL,
    old_fee_pct DECIMAL(5,2) NULL,
    new_fee_pct DECIMAL(5,2) NOT NULL,
    changed_at  DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    changed_by  SYSNAME      NOT NULL DEFAULT SUSER_SNAME()
);
GO

-- The only supported way to change a fee: validated, transactional, audited
CREATE OR ALTER PROCEDURE ref.usp_set_vendor_fee
    @vendor_id   INT,
    @new_fee_pct DECIMAL(5,2)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @new_fee_pct < 0 OR @new_fee_pct > 20
        THROW 50001, 'Fee must be between 0 and 20 percent.', 1;
    BEGIN TRY
        BEGIN TRAN;
        DECLARE @old_fee DECIMAL(5,2);
        SELECT @old_fee = platform_fee_pct FROM ref.vendor WITH (UPDLOCK) WHERE vendor_id = @vendor_id;
        IF @@ROWCOUNT = 0
            THROW 50002, 'Unknown vendor_id.', 1;
        IF @old_fee <> @new_fee_pct
        BEGIN
            UPDATE ref.vendor
               SET platform_fee_pct = @new_fee_pct, updated_at = SYSUTCDATETIME()
             WHERE vendor_id = @vendor_id;
            INSERT ref.vendor_change_log (vendor_id, old_fee_pct, new_fee_pct)
            VALUES (@vendor_id, @old_fee, @new_fee_pct);
        END
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH
END;
GO

-- Extract views: the stable contract the lakehouse reads from
CREATE OR ALTER VIEW ref.v_vendor_extract AS
SELECT vendor_id, vendor_name, platform_fee_pct, is_active, updated_at FROM ref.vendor;
GO
CREATE OR ALTER VIEW ref.v_rate_code_extract AS
SELECT rate_code_id, rate_code_desc FROM ref.rate_code;
GO