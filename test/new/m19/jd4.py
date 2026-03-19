import re

def _convert_cte_to_subquery(self, query: str) -> str:
    q = query.strip().rstrip(";").strip()

    # Not a CTE — just wrap and return
    if not re.match(r"^;?WITH\s+", q, re.IGNORECASE):
        return f"({q}) AS spark_jdbc_wrapper"

    # Step 1: Extract all CTE definitions
    # Matches: cte_name AS ( ... )
    cte_definitions = {}
    cte_section = re.search(
        r"^;?WITH\s+(.*?)\s*(?=SELECT\s)", 
        q, re.DOTALL | re.IGNORECASE
    )
    
    if not cte_section:
        return f"({q}) AS spark_jdbc_wrapper"  # fallback

    # Parse individual CTEs — handles multiple CTEs
    cte_text = cte_section.group(1)
    depth = 0
    current_cte = ""
    cte_name = ""
    i = 0

    while i < len(cte_text):
        char = cte_text[i]
        if char == "(":
            depth += 1
            if depth == 1:
                i += 1
                continue
        elif char == ")":
            depth -= 1
            if depth == 0:
                # Save completed CTE
                cte_definitions[cte_name.strip()] = current_cte.strip()
                current_cte = ""
                cte_name = ""
                i += 1
                continue
        
        if depth == 0:
            # We are between CTEs — extract next CTE name
            match = re.match(r"\s*,?\s*(\w+)\s+AS\s*\(", cte_text[i:], re.IGNORECASE)
            if match:
                cte_name = match.group(1)
                i += match.end() - 1  # Move to opening (
        else:
            current_cte += char
        i += 1

    # Step 2: Extract final SELECT
    final_select = re.search(
        r"(SELECT\s+.*$)", 
        q, re.DOTALL | re.IGNORECASE
    )

    if not final_select:
        return f"({q}) AS spark_jdbc_wrapper"  # fallback

    final_sql = final_select.group(1).strip()

    # Step 3: Replace CTE references with subqueries (innermost first)
    for cte_name, cte_body in cte_definitions.items():
        # Also replace references inside other CTE bodies
        for other_name, other_body in cte_definitions.items():
            if cte_name != other_name and re.search(rf"\b{cte_name}\b", other_body):
                cte_definitions[other_name] = re.sub(
                    rf"\b{cte_name}\b",
                    f"(SELECT * FROM ({cte_body}) AS {cte_name})",
                    other_body,
                    flags=re.IGNORECASE
                )

        # Replace in final SELECT
        final_sql = re.sub(
            rf"\b{cte_name}\b",
            f"({cte_body}) AS {cte_name}",
            final_sql,
            flags=re.IGNORECASE
        )

    return f"({final_sql}) AS spark_jdbc_wrapper"


# Usage in handler()
def handler(self):
    converted_query = self._convert_cte_to_subquery(self.SOURCE_QUERY)
    
    print("======= FINAL QUERY =======")
    print(converted_query)
    print("===========================")

    source_df = spark.read.format("jdbc") \
        .option("url", self.SOURCE_URL) \
        .option("dbtable", converted_query) \
        .option("user", self.SOURCE_USER) \
        .option("password", self.SOURCE_PSWD) \
        .option("numPartitions", 4) \
        .option("fetchsize", 100000) \
        .option("driver", driver) \
        .load()
```

---

## Summary
```
Input  → WITH cte1 AS (...), cte2 AS (...) SELECT ...
                        ↓ Python converts
Output → (SELECT ... FROM (...) AS cte1 JOIN (...) AS cte2) AS spark_jdbc_wrapper
                        ↓ Spark reads
Result → No CTE, No WITH keyword, No SQL Server confusion ✅
