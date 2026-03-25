import re

class sourceTargetCompare():

    def _clean_query(self, query: str) -> str:
        """
        Aggressively clean query loaded from config/file.
        Removes BOM, hidden chars, normalizes whitespace.
        """
        # Remove BOM character if present
        q = query.lstrip('\ufeff')
        
        # Remove Windows line endings
        q = q.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
        
        # Normalize multiple spaces to single space
        q = re.sub(r'\s+', ' ', q)
        
        # Strip and remove trailing semicolons
        q = q.strip().rstrip(';').strip()
        
        print(f"[clean_query] Cleaned: {repr(q[:100])}")
        return q


    def _is_cte_query(self, query: str) -> bool:
        """
        Strictly detects TRUE CTE query.
        
        WITH CTE_Name AS (  →  True  (real CTE)
        WITH(NOLOCK)        →  False (table hint)
        SELECT ...          →  False (plain query)
        schema.table        →  False (plain table)
        """
        q = self._clean_query(query)
        
        print(f"[is_cte] Checking: {repr(q[:100])}")
        
        # TRUE CTE must match: WITH <word> AS (
        # Table hint matches:  WITH( — bracket immediately after WITH
        result = bool(re.match(
            r"^;?WITH\s+\w+\s+AS\s*\(",
            q,
            re.IGNORECASE
        ))
        
        print(f"[is_cte] Is CTE: {result}")
        return result


    def _wrap_query(self, query: str) -> str:
        """
        Master entry point for all query types.
        
        Handles:
          1. Plain table name           → return as-is
          2. Already wrapped query      → return as-is
          3. TRUE CTE query             → convert CTE to subquery
          4. Plain SELECT               → wrap in () AS spark_jdbc_wrapper
          5. SELECT with WITH(NOLOCK)   → wrap in () AS spark_jdbc_wrapper
        """
        # Step 1: Clean the query
        q = self._clean_query(query)
        
        print(f"[wrap_query] Input (first 100): {repr(q[:100])}")

        # Case 1: Plain table name — schema.table or [db].[schema].[table]
        if re.match(r"^[\w\.\[\]]+$", q):
            print("[wrap_query] Case: Plain table name — returning as-is")
            return q

        # Case 2: Already correctly wrapped with alias
        if re.match(r"^\(.*\)\s+AS\s+\w+\s*$", q, re.DOTALL | re.IGNORECASE):
            print("[wrap_query] Case: Already wrapped — returning as-is")
            return q

        # Case 3: TRUE CTE query — convert to subquery
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
        Parses CTE query and converts to equivalent nested subquery.

        Input:
            WITH cte1 AS (SELECT ...),
                 cte2 AS (SELECT ... FROM cte1 ...)
            SELECT ... FROM cte2 alias

        Output:
            (SELECT ... FROM
                (SELECT ... FROM (SELECT ...) AS cte1) AS alias
            ) AS spark_jdbc_wrapper
        """
        # Clean again to be safe
        q = self._clean_query(q)

        # --- Step 1: Skip past WITH keyword ---
        with_match = re.match(r"^;?WITH\s+", q, re.IGNORECASE)
        if not with_match:
            raise ValueError("No WITH keyword found")
        pos = with_match.end()

        # --- Step 2: Parse each CTE name and body ---
        cte_dict  = {}   # { cte_name : cte_body }
        cte_order = []   # preserve order for substitution

        while pos < len(q):

            # Match next CTE: name AS (
            name_match = re.match(r"\s*(\w+)\s+AS\s*\(", q[pos:], re.IGNORECASE)
            if not name_match:
                break

            cte_name = name_match.group(1)
            cte_order.append(cte_name)

            # Start scanning after opening parenthesis
            body_start = pos + name_match.end()

            # Use depth counter to find matching closing parenthesis
            depth = 1
            j = body_start
            while j < len(q) and depth > 0:
                if q[j] == '(':
                    depth += 1
                elif q[j] == ')':
                    depth -= 1
                j += 1

            # Extract CTE body — everything between outer ( and )
            cte_body = q[body_start: j - 1].strip()
            cte_dict[cte_name] = cte_body

            print(f"[parse_cte] Found CTE '{cte_name}': {repr(cte_body[:80])}")

            # Move past closing ) — skip comma if more CTEs follow
            pos = j
            comma_match = re.match(r"\s*,\s*", q[pos:])
            if comma_match:
                pos += comma_match.end()
            else:
                break  # No more CTEs

        # --- Step 3: Find the final SELECT after all CTEs ---
        remaining = q[pos:].strip()

        if re.match(r"^SELECT\s+", remaining, re.IGNORECASE):
            final_select = remaining
        else:
            # Search for SELECT from current position
            select_match = re.search(r"\bSELECT\b", remaining, re.IGNORECASE)
            if select_match:
                final_select = remaining[select_match.start():].strip()
            else:
                raise ValueError("Could not find final SELECT after CTE definitions")

        print(f"[parse_cte] Final SELECT: {repr(final_select[:80])}")

        # --- Step 4: Resolve CTEs that reference earlier CTEs ---
        # Process in order — inner CTEs resolved before outer ones
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
        # Captures optional alias after CTE name
        # Skips SQL keywords that follow CTE name — ON, WHERE, AND, etc.
        result_sql = final_select
        for cte_name in cte_order:
            result_sql = re.sub(
                rf"\b{cte_name}\b(\s+(?!ON\b|WHERE\b|SET\b|AND\b|OR\b|WITH\b|INNER\b|LEFT\b|RIGHT\b|FULL\b|CROSS\b|JOIN\b|GROUP\b|ORDER\b|HAVING\b)(\w+))?",
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
            # Wrap query generically — handles all query types
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

## Complete Flow
```
handler()
    │
    └── _wrap_query(SOURCE_QUERY)
            │
            ├── _clean_query()         ← Remove BOM, \r\n, extra spaces
            │
            ├── Plain table?           → return as-is
            │   schema.table
            │
            ├── Already wrapped?       → return as-is
            │   (SELECT...) AS alias
            │
            ├── _is_cte_query()?       → _parse_and_convert_cte()
            │   WITH name AS (              │
            │                              ├── Step 1: Skip WITH
            │                              ├── Step 2: Parse CTE names/bodies
            │                              ├── Step 3: Find final SELECT
            │                              ├── Step 4: Resolve CTE→CTE refs
            │                              └── Step 5: Replace in final SELECT
            │
            └── Everything else        → (query) AS spark_jdbc_wrapper
                Plain SELECT
                WITH(NOLOCK)
