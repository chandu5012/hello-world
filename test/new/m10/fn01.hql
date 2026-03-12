WITH
-- ============================================================
-- CX6 DATA
-- ============================================================
cx6_prepared AS (
SELECT
    consumer_account_number           AS AccountNumber,
    date_opened                       AS DateOpened,
    phpdate                           AS PHPDate,
    phpvalue                          AS CX6_PHPValue,
    event_date                        AS CX6_EventDate,
    date_of_account_information       AS CX6_DOAI,

    FLOOR(months_between(add_months(trunc(current_date,'MM'),-1), phpdate)) AS IDX,

    CASE
        WHEN phpdate > add_months(trunc(current_date,'MM'),-2)
        THEN 1 ELSE 0
    END AS AccountActivelyFurnishing

FROM CX6_TABLE
),

-- ============================================================
-- ACDV DATA
-- ============================================================
acdv_prepared AS (
SELECT
    AccountNumber,
    ACDV_ID            AS ACDV_SourceID,
    PHPDate,
    EventDate          AS ACDV_EventDate,

    CASE
        WHEN PHPValue = 'C' THEN 'D'
        ELSE PHPValue
    END AS ACDV_PHPValue,

    CASE
        WHEN PHPValue = 'C' THEN 1
        ELSE 0
    END AS CValue_ACDV

FROM ACDV_TABLE
),

-- ============================================================
-- AUD DATA
-- ============================================================
aud_prepared AS (
SELECT
    AccountNumber,
    AUD_ID             AS AUD_SourceID,
    PHPDate,
    EventDate          AS AUD_EventDate,

    CASE
        WHEN PHPValue = 'C' THEN 'D'
        ELSE PHPValue
    END AS AUD_PHPValue,

    CASE
        WHEN PHPValue = 'C' THEN 1
        ELSE 0
    END AS CValue_AUD

FROM AUD_TABLE
),

-- ============================================================
-- CRA DATA
-- ============================================================
cra_prepared AS (
SELECT
    AccountNumber,
    CRA_ID             AS CRA_SourceID,
    PHPDate,
    EventDate          AS CRA_EventDate,

    CASE
        WHEN PHPValue = 'C' THEN 'D'
        ELSE PHPValue
    END AS CRA_PHPValue,

    CASE
        WHEN PHPValue = 'C' THEN 1
        ELSE 0
    END AS CValue_CRA

FROM CRA_TABLE
),

-- ============================================================
-- JOIN ALL SOURCES
-- ============================================================
base_joined AS (
SELECT

    cx.IDX,
    cx.AccountNumber,
    cx.DateOpened,
    cx.AccountActivelyFurnishing,
    cx.PHPDate,

    cx.CX6_PHPValue,
    cx.CX6_EventDate,
    cx.CX6_DOAI,

    acdv.ACDV_PHPValue,
    acdv.CValue_ACDV,
    acdv.ACDV_EventDate,
    acdv.ACDV_SourceID,

    aud.AUD_PHPValue,
    aud.CValue_AUD,
    aud.AUD_EventDate,
    aud.AUD_SourceID,

    cra.CRA_PHPValue,
    cra.CValue_CRA,
    cra.CRA_EventDate,
    cra.CRA_SourceID

FROM cx6_prepared cx

LEFT JOIN acdv_prepared acdv
ON cx.AccountNumber = acdv.AccountNumber
AND cx.PHPDate = acdv.PHPDate

LEFT JOIN aud_prepared aud
ON cx.AccountNumber = aud.AccountNumber
AND cx.PHPDate = aud.PHPDate

LEFT JOIN cra_prepared cra
ON cx.AccountNumber = cra.AccountNumber
AND cx.PHPDate = cra.PHPDate
),

-- ============================================================
-- EVENT CALCULATIONS
-- ============================================================
php_extended AS (
SELECT
    b.*,

    GREATEST(
        b.CX6_DOAI,
        b.ACDV_EventDate,
        b.AUD_EventDate,
        b.CRA_EventDate
    ) AS LastEventDate,

    CONCAT(
        COALESCE(b.CX6_PHPValue,''),
        COALESCE(b.ACDV_PHPValue,''),
        COALESCE(b.AUD_PHPValue,''),
        COALESCE(b.CRA_PHPValue,'')
    ) AS AllPHPValues

FROM base_joined b
),

php_extended_more AS (
SELECT
    e.*,

    CONCAT(
        CASE WHEN e.CX6_DOAI = e.LastEventDate THEN COALESCE(e.CX6_PHPValue,'') ELSE '' END,
        CASE WHEN e.ACDV_EventDate = e.LastEventDate THEN COALESCE(e.ACDV_PHPValue,'') ELSE '' END,
        CASE WHEN e.AUD_EventDate = e.LastEventDate THEN COALESCE(e.AUD_PHPValue,'') ELSE '' END,
        CASE WHEN e.CRA_EventDate = e.LastEventDate THEN COALESCE(e.CRA_PHPValue,'') ELSE '' END
    ) AS LastEventPHPValues

FROM php_extended e
)

-- ============================================================
-- FINAL RULE ENGINE
-- ============================================================
SELECT

AccountNumber,
DateOpened,
PHPDate,

CX6_PHPValue,
CX6_EventDate,
CX6_DOAI,

ACDV_PHPValue,
ACDV_EventDate,
ACDV_SourceID,

AUD_PHPValue,
AUD_EventDate,
AUD_SourceID,

CRA_PHPValue,
CRA_EventDate,
CRA_SourceID,

CASE
    WHEN length(AllPHPValues) > 1 THEN 1
    ELSE 0
END AS HasConflicts,

CASE

WHEN AllPHPValues = '' THEN ''

WHEN regexp_replace(AllPHPValues, substr(AllPHPValues,1,1),'') = ''
THEN substr(AllPHPValues,1,1)

WHEN CRA_PHPValue IS NOT NULL
THEN CRA_PHPValue

WHEN AccountActivelyFurnishing = 1
AND IDX < 24
AND CX6_PHPValue IS NOT NULL
THEN CX6_PHPValue

WHEN length(LastEventPHPValues) > 0
AND regexp_replace(LastEventPHPValues, substr(LastEventPHPValues,1,1),'') = ''
THEN substr(LastEventPHPValues,1,1)

WHEN length(LastEventPHPValues) > 0
THEN 'D'

ELSE '?'

END AS CalculatedPHP

FROM php_extended_more
WHERE PHPDate >= trunc(DateOpened,'MM')
;
