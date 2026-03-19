def split_sqlserver_cte_query(self, full_sql):
    """
    Split SQL Server CTE query into:
      prepare_query -> WITH ... CTE block
      main_query    -> final SELECT / INSERT / etc.

    For normal query, returns:
      (None, cleaned_sql)
    """

    if full_sql is None or not str(full_sql).strip():
        raise Exception("SOURCE_QUERY is empty")

    sql = str(full_sql).strip().rstrip(";")

    # remove leading semicolon for ;WITH
    if sql[:1] == ";":
        sql = sql[1:].lstrip()

    upper_sql = sql.upper()

    # normal query
    if not upper_sql.startswith("WITH "):
        return None, sql

    length = len(sql)
    i = 0
    depth = 0
    in_string = False

    # find first '(' after WITH cte_name AS
    first_paren_found = False

    while i < length:
        ch = sql[i]

        if in_string:
            if ch == "'":
                if i + 1 < length and sql[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue

        if ch == "'":
            in_string = True
            i += 1
            continue

        if ch == "(":
            depth += 1
            first_paren_found = True
        elif ch == ")":
            depth -= 1

            # end of one CTE block
            if first_paren_found and depth == 0:
                j = i + 1

                # skip spaces/newlines/tabs
                while j < length and sql[j] in (" ", "\n", "\r", "\t"):
                    j += 1

                # if next token is comma, then another CTE starts
                if j < length and sql[j] == ",":
                    i = j + 1
                    continue

                # otherwise remaining SQL is main query
                prepare_query = sql[:i + 1].strip()
                main_query = sql[i + 1:].strip()

                if not prepare_query.upper().startswith("WITH "):
                    raise Exception("Invalid prepare query split")

                if not main_query:
                    raise Exception("Main query is empty after CTE block")

                return prepare_query, main_query

        i += 1

    raise Exception("Unable to split SQL Server CTE query")
