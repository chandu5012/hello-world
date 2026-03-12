SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dsas_conformed.crcoe_non_cx6_php_acdv_table
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
        ACDVREQ_ACCT_NUM,
        ACDVControlNumber,
        ACDVRESP_RESPONSE_DATE_TIME,
        date_opened,
        ACDVRESP_SEVEN_YEAR_PAY_HIST,
        idx,
        to_date(ACDVRESP_RESPONSE_DATE_TIME) AS EventDate,
        add_months(trunc(to_date(ACDVRESP_RESPONSE_DATE_TIME), 'MM'), -(idx + 1)) AS PHPDate,
        substr(ACDVRESP_SEVEN_YEAR_PAY_HIST, idx + 1, 1) AS PHPValue,
        CAST(TRIM(ACDVREQ_ACCT_NUM) AS BIGINT) AS AccountNumber
    FROM hive_dsas_fnsh_sanitized.eoscar_acdvarchive_hist
    JOIN cteNumbers
      ON 1 = 1
),

filtered_data AS (
    SELECT *
    FROM base_data
    WHERE
        coalesce(PHPValue, '') NOT IN ('', '-')
        AND coalesce(ACDVRESP_SEVEN_YEAR_PAY_HIST, '') <> ''
        AND regexp_replace(ACDVRESP_SEVEN_YEAR_PAY_HIST, '-', '') <> ''
        AND ACDVRESP_RESPONSE_DATE_TIME IS NOT NULL
        AND EventDate > add_months(current_date, -85)
        AND date_opened IS NOT NULL
        AND AccountNumber IS NOT NULL
),

cteACDVData AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY ACDVREQ_ACCT_NUM, PHPDate
            ORDER BY EventDate DESC
        ) AS RowNum,
        AccountNumber,
        ACDVControlNumber AS ACDV_ID,
        EventDate,
        PHPDate,
        PHPValue
    FROM filtered_data
),

base_r1 AS (
    SELECT
        AccountNumber,
        ACDV_ID,
        EventDate,
        PHPDate,
        PHPValue
    FROM cteACDVData
    WHERE RowNum = 1
),

chk_data AS (
    SELECT
        AccountNumber,
        PHPDate,
        PHPValue
    FROM cteACDVData
    WHERE RowNum > 1
),

final_data AS (
    SELECT
        b.AccountNumber,
        b.ACDV_ID,
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
    ACDV_ID,
    EventDate,
    PHPDate,
    PHPValue,
    date_format(PHPDate, 'yyyy-MM') AS php_month
FROM final_data
WHERE PHPDate > add_months(current_date, -85);
