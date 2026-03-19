import re

def _convert_cte_to_subquery(self, query: str) -> str:
    """
    Pure Python CTE to Subquery converter.
    No external packages needed.
    Works on enterprise PySpark clusters.
    """
    q = query.strip().rstrip(";").strip()

    # Not a CTE — just wrap and return
    if not re.match(r"^;?WITH\s+", q, re.IGNORECASE):
        return f"({q}) AS spark_jdbc_wrapper"

    try:
        return self._parse_and_convert_cte(q)
    except Exception as e:
        print(f"[WARN] CTE conversion failed: {e}")
        return f"({q}) AS spark_jdbc_wrapper"


def _parse_and_convert_cte(self, q: str) -> str:
    """
    Step 1: Parse all CTE names and bodies using bracket depth tracking.
    Step 2: Find the final SELECT.
    Step 3: Substitute CTE names with inline subqueries.
    """

    # --- Step 1: Skip past WITH keyword ---
    with_match = re.match(r"^;?WITH\s+", q, re.IGNORECASE)
    pos = with_match.end()

    # --- Step 2: Parse each CTE block ---
    cte_dict  = {}   # {name: body}
    cte_order = []   # maintain order for substitution

    while pos < len(q):
        # Match CTE name followed by AS (
        name_match = re.match(r"\s*(\w+)\s+AS\s*\(", q[pos:], re.IGNORECASE)
        if not name_match:
            break

        cte_name = name_match.group(1)
        cte_order.append(cte_name)

        # Start after opening parenthesis
        body_start = pos + name_match.end()

        # Track depth to find matching closing parenthesis
        depth = 1
        j = body_start
        while j < len(q) and depth > 0:
            if q[j] == '(':
                depth += 1
            elif q[j] == ')':
                depth -= 1
            j += 1

        # Extract CTE body (without outer parentheses)
        cte_body = q[body_start : j - 1].strip()
        cte_dict[cte_name] = cte_body

        print(f"[DEBUG] Found CTE: {cte_name}")
        print(f"[DEBUG] CTE Body: {cte_body[:100]}...")

        # Move past closing ) — skip comma if present
        pos = j
        comma_match = re.match(r"\s*,\s*", q[pos:])
        if comma_match:
            pos += comma_match.end()
        else:
            break  # No more CTEs

    # --- Step 3: Find the final SELECT (outside all parentheses) ---
    final_select = q[pos:].strip()

    # Verify it starts with SELECT
    if not re.match(r"^SELECT\s+", final_select, re.IGNORECASE):
        # Try to find SELECT from current position
        select_match = re.search(r"\bSELECT\b", q[pos:], re.IGNORECASE)
        if select_match:
            final_select = q[pos + select_match.start():].strip()
        else:
            raise ValueError("Could not find final SELECT statement")

    print(f"[DEBUG] Final SELECT: {final_select[:100]}...")

    # --- Step 4: Resolve CTEs that reference other CTEs ---
    # Process in order so inner CTEs get resolved first
    for i, cte_name in enumerate(cte_order):
        body = cte_dict[cte_name]
        # Replace any earlier CTE references inside this body
        for earlier_cte in cte_order[:i]:
            body = re.sub(
                rf"\b{earlier_cte}\b",
                f"({cte_dict[earlier_cte]}) AS {earlier_cte}",
                body,
                flags=re.IGNORECASE
            )
        cte_dict[cte_name] = body

    # --- Step 5: Replace CTE names in final SELECT ---
    result_sql = final_select
    for cte_name in cte_order:
        result_sql = re.sub(
            rf"\b{cte_name}\b",
            f"({cte_dict[cte_name]}) AS {cte_name}",
            result_sql,
            flags=re.IGNORECASE
        )

    final_query = f"({result_sql}) AS spark_jdbc_wrapper"

    print("======= FINAL CONVERTED QUERY =======")
    print(final_query)
    print("=====================================")

    return final_query
