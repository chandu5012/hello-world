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

cteACDVData AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                ACDVREQ_ACCT_NUM,
                add_months(trunc(CAST(ACDVRESP_RESPONSE_DATE_TIME AS DATE), 'MM'), -(NUM.idx + 1))
            ORDER BY
                CAST(ACDVRESP_RESPONSE_DATE_TIME AS DATE) DESC
        ) AS RowNum,

        CAST(TRIM(ACDVREQ_ACCT_NUM) AS BIGINT) AS AccountNumber,
        ACDVControlNumber AS ACDV_ID,
        CAST(ACDVRESP_RESPONSE_DATE_TIME AS DATE) AS EventDate,
        add_months(trunc(CAST(ACDVRESP_RESPONSE_DATE_TIME AS DATE), 'MM'), -(NUM.idx + 1)) AS PHPDate,
        substr(ACDVRESP_SEVEN_YEAR_PAY_HIST, NUM.idx + 1, 1) AS PHPValue

    FROM hive_dsas_fnsh_sanitized.eoscar_acdvarchive_hist
    JOIN cteNumbers NUM
      ON 1 = 1
    WHERE
        coalesce(substr(ACDVRESP_SEVEN_YEAR_PAY_HIST, NUM.idx + 1, 1), '') NOT IN ('', '-')
        AND coalesce(ACDVRESP_SEVEN_YEAR_PAY_HIST, '') <> ''
        AND regexp_replace(ACDVRESP_SEVEN_YEAR_PAY_HIST, '-', '') <> ''
        AND ACDVRESP_RESPONSE_DATE_TIME IS NOT NULL
        AND CAST(ACDVRESP_RESPONSE_DATE_TIME AS DATE) > add_months(current_date, -85)
        AND date_opened IS NOT NULL
        AND CAST(TRIM(ACDVREQ_ACCT_NUM) AS BIGINT) IS NOT NULL
)

SELECT
    AccountNumber,
    ACDV_ID,
    EventDate,
    PHPDate,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM cteACDVData CHK
            WHERE CHK.AccountNumber = cteACDVData.AccountNumber
              AND CHK.PHPDate = cteACDVData.PHPDate
              AND CHK.RowNum > 1
              AND CHK.PHPValue <> cteACDVData.PHPValue
        ) THEN 'C'
        ELSE cteACDVData.PHPValue
    END AS PHPValue,
    date_format(PHPDate, 'yyyy-MM') AS php_month
FROM cteACDVData
WHERE RowNum = 1
  AND PHPDate > add_months(current_date, -85);
