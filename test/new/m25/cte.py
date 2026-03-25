import re

class sourceTargetCompare():

    def _is_cte_query(self, query: str) -> bool:
        """
        Detects if query is a TRUE CTE query.
        
        WITH CTE_Name AS (  → True  (CTE)
        WITH(NOLOCK)        → False (table hint)
        SELECT ...          → False (plain query)
        schema.table        → False (plain table)
        """
        q = query.strip().rstrip(";").strip()
        return bool(re.match(
            r"^;?WITH\s+\w+\s+AS\s*\(",
            q,
            re.IGNORECASE
        ))

    def _wrap_query(self, query: str) -> str:
        """
        Master method — entry point for all query types.
        Detects query type and applies correct handling.
        
        Handles:
            1. Plain table name          → return as-is
            2. Already wrapped query     → return as-is
            3. CTE query (WITH x AS ...) → convert to subquery
            4. Plain SELECT              → wrap in () AS alias
            5. SELECT with WITH(NOLOCK)  → wrap in () AS alias
        """
        q = query.strip().rstrip(";").strip()

        print(f"[wrap_query] RAW INPUT:\n{q}\n")

        # Case 1: Plain table name — no spaces, just schema.table
        if re.match(r"^[\w\.\[\]]+$", q):
            print("[wrap_query] Case: Plain table name — returning as-is")
            return q

        # Case 2: Already correctly wrapped
        if re.match(r"^\(.*\)\s+AS\s+\w+\s*$", q, re.DOTALL | re.IGNORECASE):
            print("[wrap_query] Case: Already wrapped — returning as-is")
            return q

        # Case 3: CTE query — convert to subquery
        if self._is_cte_query(q):
            print("[wrap_query] Case: CTE query — converting to subquery")
            try:
                return self._parse_and_convert_cte(q)
            except Exception as e:
                print(f"[wrap_query] CTE conversion failed: {e} — falling back to plain wrap")
                return f"({q}) AS spark_jdbc_wrapper"

        # Case 4 & 5: Plain SELECT or SELECT with WITH(NOLOCK) — just wrap
        print("[wrap_query] Case: Plain SELECT or WITH(NOLOCK) — wrapping as-is")
        return f"({q}) AS spark_jdbc_wrapper"


    def _parse_and_convert_cte(self, q: str) -> str:
        """
        Converts CTE query to equivalent subquery.
        
        Input:
            WITH cte1 AS (...),
                 cte2 AS (... cte1 ...)
            SELECT ... FROM cte2

        Output:
            (SELECT ... FROM
                (... (SELECT ...) AS cte1 ...) AS cte2
            ) AS spark_jdbc_wrapper
        """

        # --- Step 1: Skip past WITH keyword ---
        with_match = re.match(r"^;?WITH\s+", q, re.IGNORECASE)
        pos = with_match.end()

        # --- Step 2: Parse each CTE name and body ---
        cte_dict  = {}   # { cte_name: cte_body }
        cte_order = []   # preserve order for substitution

        while pos < len(q):

            # Match CTE name followed by AS (
            name_match = re.match(r"\s*(\w+)\s+AS\s*\(", q[pos:], re.IGNORECASE)
            if not name_match:
                break

            cte_name = name_match.group(1)
            cte_order.append(cte_name)

            # Start after opening parenthesis
            body_start = pos + name_match.end()

            # Track parenthesis depth to find matching closing )
            depth = 1
            j = body_start
            while j < len(q) and depth > 0:
                if q[j] == '(':
                    depth += 1
                elif q[j] == ')':
                    depth -= 1
                j += 1

            # Extract CTE body without outer parentheses
            cte_body = q[body_start: j - 1].strip()
            cte_dict[cte_name] = cte_body

            print(f"[parse_cte] Found CTE '{cte_name}': {cte_body[:80]}...")

            # Move past closing ) — skip comma if more CTEs follow
            pos = j
            comma_match = re.match(r"\s*,\s*", q[pos:])
            if comma_match:
                pos += comma_match.end()
            else:
                break  # No more CTEs

        # --- Step 3: Find the final SELECT after all CTEs ---
        final_select = q[pos:].strip()

        if not re.match(r"^SELECT\s+", final_select, re.IGNORECASE):
            select_match = re.search(r"\bSELECT\b", q[pos:], re.IGNORECASE)
            if select_match:
                final_select = q[pos + select_match.start():].strip()
            else:
                raise ValueError("Could not find final SELECT after CTEs")

        print(f"[parse_cte] Final SELECT: {final_select[:80]}...")

        # --- Step 4: Resolve CTEs that reference earlier CTEs ---
        # Process in order so inner CTE references get resolved first
        for i, cte_name in enumerate(cte_order):
            body = cte_dict[cte_name]
            for earlier_cte in cte_order[:i]:
                body = re.sub(
                    rf"\b{earlier_cte}\b(\s+(?!AS\s*\()(\w+))?",
                    lambda m, ec=earlier_cte: (
                        f"({cte_dict[ec]}) AS {m.group(2).strip()}"
                        if m.group(2) and m.group(2).strip()
                        else f"({cte_dict[ec]}) AS {ec}"
                    ),
                    body,
                    flags=re.IGNORECASE
                )
            cte_dict[cte_name] = body

        # --- Step 5: Replace CTE references in final SELECT ---
        # Capture optional alias after CTE name
        # Skip SQL keywords — ON, WHERE, AND, OR, SET, WITH
        result_sql = final_select
        for cte_name in cte_order:
            result_sql = re.sub(
                rf"\b{cte_name}\b(\s+(?!ON\b|WHERE\b|SET\b|AND\b|OR\b|WITH\b|INNER\b|LEFT\b|RIGHT\b|JOIN\b)(\w+))?",
                lambda m, cn=cte_name: (
                    f"({cte_dict[cn]}) AS {m.group(2).strip()}"
                    if m.group(2) and m.group(2).strip()
                    else f"({cte_dict[cn]}) AS {cn}"
                ),
                result_sql,
                flags=re.IGNORECASE
            )

        final_query = f"({result_sql}) AS spark_jdbc_wrapper"

        print("======= FINAL CONVERTED QUERY =======")
        print(final_query)
        print("=====================================")

        return final_query


    def handler(self):
        log.info('INFO ==========> connecting the source ===========>')

        try:
            # Apply generic query wrapper
            wrapped_query = self._wrap_query(self.SOURCE_QUERY)

            print("======= QUERY SENT TO SPARK =======")
            print(wrapped_query)
            print("===================================")

            source_df = spark.read.format("jdbc") \
                .option("url",           self.SOURCE_URL) \
                .option("dbtable",       wrapped_query) \
                .option("user",          self.SOURCE_USER) \
                .option("password",      self.SOURCE_PSWD) \
                .option("numPartitions", 4) \
                .option("fetchsize",     100000) \
                .option("driver",        driver) \
                .load()

            log.info('INFO ==========> data extracted successfully from source ===========>')

        except Exception as e:
            log.error(f"Exception is identified - please check error message :{str(e)}")
            raise
```

---

## How Each Query Type is Handled
```
┌─────────────────────────────────────────────────┬─────────────────────────────────────┐
│ Input Query                                     │ Output                              │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ schema.table                                    │ schema.table                        │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ SELECT * FROM tbl                               │ (SELECT * FROM tbl)                 │
│                                                 │  AS spark_jdbc_wrapper              │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ SELECT * FROM tbl WITH(NOLOCK)                  │ (SELECT * FROM tbl WITH(NOLOCK))    │
│                                                 │  AS spark_jdbc_wrapper              │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ WITH cte AS (...)                               │ (SELECT ... FROM                    │
│ SELECT * FROM cte                               │  (...) AS cte)                      │
│                                                 │  AS spark_jdbc_wrapper              │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ WITH cte1 AS (...),                             │ (SELECT ... FROM                    │
│      cte2 AS (... cte1 ...)                     │  (... (...) AS cte1) AS cte2)       │
│ SELECT * FROM cte2                              │  AS spark_jdbc_wrapper              │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ WITH cte AS (                                   │ (SELECT ... FROM                    │
│   SELECT * FROM tbl WITH(NOLOCK)                │  (SELECT * FROM tbl WITH(NOLOCK))   │
│ ) SELECT * FROM cte                             │  AS cte) AS spark_jdbc_wrapper      │
├─────────────────────────────────────────────────┼─────────────────────────────────────┤
│ (SELECT * FROM tbl) AS t                        │ (SELECT * FROM tbl) AS t            │
│ (already wrapped)                               │ (unchanged)                         │
└─────────────────────────────────────────────────┴─────────────────────────────────────┘
```

---

## Flow Diagram
```
_wrap_query(query)
       │
       ├── Plain table name?        → return as-is
       │   schema.table
       │
       ├── Already wrapped?         → return as-is
       │   (SELECT ...) AS alias
       │
       ├── Starts with              → _parse_and_convert_cte()
       │   WITH <name> AS ( ?          │
       │                              ├── Parse CTE names & bodies
       │                              ├── Find final SELECT
       │                              ├── Resolve CTE→CTE references
       │                              └── Replace CTEs with subqueries
       │
       └── Everything else          → (query) AS spark_jdbc_wrapper
           Plain SELECT
           WITH(NOLOCK) queries
