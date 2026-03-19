import re

def _convert_cte_to_subquery(self, query: str) -> str:
    q = query.strip().rstrip(";").strip()

    if not re.match(r"^;?WITH\s+", q, re.IGNORECASE):
        return f"({q}) AS spark_jdbc_wrapper"

    try:
        return self._parse_and_convert_cte(q)
    except Exception as e:
        print(f"[WARN] CTE conversion failed: {e}")
        return f"({q}) AS spark_jdbc_wrapper"


def _parse_and_convert_cte(self, q: str) -> str:

    # --- Step 1: Skip past WITH keyword ---
    with_match = re.match(r"^;?WITH\s+", q, re.IGNORECASE)
    pos = with_match.end()

    # --- Step 2: Parse each CTE block ---
    cte_dict  = {}
    cte_order = []

    while pos < len(q):
        name_match = re.match(r"\s*(\w+)\s+AS\s*\(", q[pos:], re.IGNORECASE)
        if not name_match:
            break

        cte_name = name_match.group(1)
        cte_order.append(cte_name)

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

        cte_body = q[body_start: j - 1].strip()
        cte_dict[cte_name] = cte_body

        print(f"[DEBUG] Found CTE: {cte_name}")

        pos = j
        comma_match = re.match(r"\s*,\s*", q[pos:])
        if comma_match:
            pos += comma_match.end()
        else:
            break

    # --- Step 3: Find final SELECT ---
    final_select = q[pos:].strip()
    if not re.match(r"^SELECT\s+", final_select, re.IGNORECASE):
        select_match = re.search(r"\bSELECT\b", q[pos:], re.IGNORECASE)
        if select_match:
            final_select = q[pos + select_match.start():].strip()
        else:
            raise ValueError("Could not find final SELECT")

    # --- Step 4: Resolve CTEs referencing other CTEs ---
    for i, cte_name in enumerate(cte_order):
        body = cte_dict[cte_name]
        for earlier_cte in cte_order[:i]:
            # ✅ KEY FIX: Handle optional alias after CTE name
            # Matches: CTE_NAME alias  or  CTE_NAME
            body = re.sub(
                rf"\b{earlier_cte}\b(\s+(?!AS\s*\()(\w+))?",
                lambda m: f"({cte_dict[earlier_cte]}) AS {m.group(2).strip() if m.group(2) else earlier_cte}",
                body,
                flags=re.IGNORECASE
            )
        cte_dict[cte_name] = body

    # --- Step 5: Replace CTE names in final SELECT ---
    # ✅ KEY FIX: Capture alias after CTE name and use it correctly
    result_sql = final_select
    for cte_name in cte_order:
        result_sql = re.sub(
            rf"\b{cte_name}\b(\s+(?!ON\b|WHERE\b|SET\b|AND\b|OR\b)(\w+))?",
            lambda m: f"({cte_dict[cte_name]}) AS {m.group(2).strip() if m.group(2) and m.group(2).strip() else cte_name}",
            result_sql,
            flags=re.IGNORECASE
        )

    final_query = f"({result_sql}) AS spark_jdbc_wrapper"

    print("======= FINAL CONVERTED QUERY =======")
    print(final_query)
    print("=====================================")

    return final_query
