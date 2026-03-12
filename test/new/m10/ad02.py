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

cteAUDData AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                acct_num,
                add_months(trunc(CAST(date_created AS DATE), 'MM'), -(NUM.idx + 1))
            ORDER BY
                CAST(date_created AS DATE) DESC
        ) AS RowNum,
        CAST(TRIM(acct_num) AS BIGINT) AS AccountNumber,
        aud_id AS AUD_ID,
        CAST(date_created AS DATE) AS EventDate,
        add_months(trunc(CAST(date_created AS DATE), 'MM'), -(NUM.idx + 1)) AS PHPDate,
        substr(seven_year_payment_history, NUM.idx + 1, 1) AS PHPValue
    FROM hive_dsas_fnsh_sanitized.eoscar_aud_hist
    JOIN cteNumbers NUM
      ON 1 = 1
    WHERE
        coalesce(substr(seven_year_payment_history, NUM.idx + 1, 1), '') NOT IN ('', '-')
        AND coalesce(seven_year_payment_history, '') <> ''
        AND regexp_replace(seven_year_payment_history, '-', '') <> ''
        AND date_created IS NOT NULL
        AND CAST(date_created AS DATE) > add_months(current_date, -85)
        AND date_opened IS NOT NULL
        AND CAST(TRIM(acct_num) AS BIGINT) IS NOT NULL
),

cteAUDChk AS (
    SELECT DISTINCT
        a.AccountNumber,
        a.PHPDate
    FROM cteAUDData a
    JOIN cteAUDData b
      ON a.AccountNumber = b.AccountNumber
     AND a.PHPDate = b.PHPDate
     AND b.RowNum > 1
     AND b.PHPValue <> a.PHPValue
    WHERE a.RowNum = 1
)

SELECT
    a.AccountNumber,
    a.AUD_ID,
    a.EventDate,
    a.PHPDate,
    CASE
        WHEN c.AccountNumber IS NOT NULL THEN 'C'
        ELSE a.PHPValue
    END AS PHPValue,
    date_format(a.PHPDate, 'yyyy-MM') AS php_month
FROM cteAUDData a
LEFT JOIN cteAUDChk c
  ON a.AccountNumber = c.AccountNumber
 AND a.PHPDate = c.PHPDate
WHERE a.RowNum = 1
  AND a.PHPDate > add_months(current_date, -85);
