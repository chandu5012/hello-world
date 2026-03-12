SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dsas_conformed.crcoe_non_cx6_php_aud_table
PARTITION (php_month)
WITH cteNumbers AS (
    SELECT stack(
        84,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
        30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
        40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
        50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
        60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
        70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
        80, 81, 82, 83
    ) AS idx
),

base_data AS (
    SELECT
        acct_num,
        aud_id,
        date_created,
        date_opened,
        seven_year_payment_history,
        idx,
        to_date(date_created) AS EventDate,
        add_months(trunc(to_date(date_created), 'MM'), -(idx + 1)) AS PHPDate,
        substr(seven_year_payment_history, idx + 1, 1) AS PHPValue,
        CAST(TRIM(acct_num) AS BIGINT) AS AccountNumber
    FROM hive_dsas_fnsh_sanitized.eoscar_aud_hist
    JOIN cteNumbers
      ON 1 = 1
),

filtered_data AS (
    SELECT *
    FROM base_data
    WHERE
        coalesce(PHPValue, '') NOT IN ('', '-')
        AND coalesce(seven_year_payment_history, '') <> ''
        AND regexp_replace(seven_year_payment_history, '-', '') <> ''
        AND date_created IS NOT NULL
        AND EventDate > add_months(current_date, -85)
        AND date_opened IS NOT NULL
        AND AccountNumber IS NOT NULL
),

cteAUDData AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY acct_num, PHPDate
            ORDER BY EventDate DESC
        ) AS RowNum,
        AccountNumber,
        aud_id AS AUD_ID,
        EventDate,
        PHPDate,
        PHPValue
    FROM filtered_data
),

base_r1 AS (
    SELECT
        AccountNumber,
        AUD_ID,
        EventDate,
        PHPDate,
        PHPValue
    FROM cteAUDData
    WHERE RowNum = 1
),

chk_data AS (
    SELECT
        AccountNumber,
        PHPDate,
        PHPValue
    FROM cteAUDData
    WHERE RowNum > 1
),

final_data AS (
    SELECT
        b.AccountNumber,
        b.AUD_ID,
        b.EventDate,
        b.PHPDate,
        CASE
            WHEN c.AccountNumber IS NOT NULL THEN 'C'
            ELSE b.PHPValue
        END AS PHPValue
    FROM base_r1 b
    LEFT JOIN chk_data c
      ON b.AccountNumber = c.AccountNumber
     AND b.PHPDate = c.PHPDate
     AND b.PHPValue <> c.PHPValue
)

SELECT DISTINCT
    AccountNumber,
    AUD_ID,
    EventDate,
    PHPDate,
    PHPValue,
    date_format(PHPDate, 'yyyy-MM') AS php_month
FROM final_data
WHERE PHPDate > add_months(current_date, -85);
